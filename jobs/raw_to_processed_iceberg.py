"""
raw files (3 zone) -> processed_events Iceberg.

bronze layer는 event-type 별로 plain parquet 3 zone으로 쌓여 있고
(impressions / clicks / conversions), silver layer는 event_id 기준으로 join 해서
한 행으로 조립한다.

  raw/impressions/  + raw/clicks/  + raw/conversions/  ──>  processed_events (Iceberg)

- impression이 base. click / conversion은 left-join으로 합류한다.
- 같은 event_id에 conversion이 늦게 도착(=raw/conversions/에 새 파일이 생기)면
  다음 MERGE 실행 시 source 행이 conversion=1으로 바뀌어 WHEN MATCHED 분기가 발화된다.
"""

import argparse


def build_spark(app_name, catalog_name, warehouse):
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            f"spark.sql.catalog.{catalog_name}.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .config(
            f"spark.sql.catalog.{catalog_name}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse)
        .getOrCreate()
    )


def ensure_processed_table(spark, target_table, catalog_name, database):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{database}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            event_id STRING,
            event_date DATE,
            uid STRING,
            campaign INT,
            click INT,
            conversion INT,
            conversion_delay_sec BIGINT,
            cost DOUBLE,
            updated_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (event_date)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.update.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read',
            'write.target-file-size-bytes' = '134217728'
        )
        """
    )


def read_zone(spark, path, raw_format):
    """zone 의 모든 parquet 을 읽되, *Streaming sink 의 _spark_metadata 를 우회* 한다.

    Spark Structured Streaming sink 가 만든 `_spark_metadata` 디렉토리는 *committed*
    파일 목록을 추적한다. 이 디렉토리가 있으면 `spark.read.parquet(path)` 가 그 안의
    파일만 보고 *물리적으로 S3 에 있는 다른 parquet 은 invisible* 해진다.
    streaming 잡이 재기동되거나 commit 시점이 어긋나면 옛 commit 만 인덱스되어
    새 데이터가 보이지 않는 현상이 생긴다.

    해결: `raw_date=*/raw_hour=*` 패턴으로 *직접 path glob 읽기* — `_spark_metadata`
    를 거치지 않고 raw_date / raw_hour 파티션 디렉토리 아래 parquet 을 그대로 읽는다.
    """
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    glob_path = f"{path.rstrip('/')}/raw_date=*/raw_hour=*"
    try:
        df = spark.read.format(raw_format).load(glob_path)
        _ = df.schema  # eager 로 schema 확정 → 비어있으면 여기서 AnalysisException
        return df
    except Exception as exc:
        msg = str(exc)
        if "Unable to infer schema" in msg or "Path does not exist" in msg:
            print(f"warn: zone empty or missing, using empty DF: {glob_path}  ({exc.__class__.__name__})")
            empty_schema = StructType(
                [
                    StructField("event_id", StringType()),
                    StructField("event_timestamp", TimestampType()),
                    StructField("ingest_ts", TimestampType()),
                    StructField("uid", StringType()),
                    StructField("campaign", IntegerType()),
                    StructField("cost", DoubleType()),
                    StructField("conversion_delay_sec", LongType()),
                ]
            )
            return spark.createDataFrame([], empty_schema)
        raise


def transform_raw(impression_df, click_df, conversion_df):
    """3 zone을 event_id로 join해 silver row를 조립한다.

    각 zone은 streaming 재처리 등으로 같은 event_id가 중복 적재될 수 있으므로
    event_id 기준 최신 ingest_ts row 하나만 골라 source-of-truth로 쓴다.
    """
    from pyspark.sql.functions import (
        coalesce,
        col,
        current_timestamp,
        lit,
        row_number,
        to_date,
    )
    from pyspark.sql.window import Window

    def latest_per_event(df):
        win = Window.partitionBy("event_id").orderBy(col("ingest_ts").desc_nulls_last())
        return df.withColumn("_rn", row_number().over(win)).filter(col("_rn") == 1).drop("_rn")

    imp = latest_per_event(impression_df).select(
        col("event_id"),
        col("event_timestamp").alias("imp_event_ts"),
        col("uid"),
        col("campaign").cast("int").alias("campaign"),
        col("cost").cast("double").alias("cost"),
    )

    clk = latest_per_event(click_df).select(
        col("event_id"),
        lit(1).alias("click_flag"),
    )

    cnv = latest_per_event(conversion_df).select(
        col("event_id"),
        lit(1).alias("conversion_flag"),
        col("conversion_delay_sec").cast("bigint").alias("conversion_delay_sec"),
    )

    joined = (
        imp.join(clk, "event_id", "left")
        .join(cnv, "event_id", "left")
    )

    return joined.select(
        col("event_id"),
        to_date(col("imp_event_ts")).alias("event_date"),
        col("uid"),
        col("campaign"),
        coalesce(col("click_flag"), lit(0)).cast("int").alias("click"),
        coalesce(col("conversion_flag"), lit(0)).cast("int").alias("conversion"),
        col("conversion_delay_sec"),
        col("cost"),
        current_timestamp().alias("updated_at"),
    )


def full_refresh(spark, transformed_df, target_table):
    transformed_df.writeTo(target_table).overwritePartitions()


def _filter_window(transformed_df, merge_window_days):
    return transformed_df.filter(
        f"event_date >= current_date() - INTERVAL {merge_window_days} DAYS"
    )


def insert_new_events(spark, transformed_df, target_table, merge_window_days):
    """source 의 *새* event_id 만 INSERT.

    MERGE 의 'WHEN NOT MATCHED THEN INSERT' 분기를 *별도 task* 로 분리.
    LEFT ANTI JOIN 으로 target 에 없는 event_id 만 추린다 — 정상 흐름의 대부분.
    """
    filtered = _filter_window(transformed_df, merge_window_days)
    filtered.createOrReplaceTempView("source_processed_events_insert")
    spark.sql(
        f"""
        INSERT INTO {target_table}
        SELECT
          s.event_id,
          s.event_date,
          s.uid,
          s.campaign,
          s.click,
          s.conversion,
          s.conversion_delay_sec,
          s.cost,
          s.updated_at
        FROM source_processed_events_insert s
        LEFT ANTI JOIN {target_table} t
          ON t.event_id = s.event_id
        """
    )


def update_late_arrivals(spark, transformed_df, target_table, merge_window_days):
    """source 의 *기존* event_id 에 conversion 이 새로 도착한 경우만 UPDATE.

    MERGE 의 'WHEN MATCHED THEN UPDATE' 분기를 *별도 task* 로 분리.
    INSERT-only 흐름은 conversion=0 으로 들어왔을 row 를 늦게 도착한 conversion 으로
    갱신하는 것이 이 단계의 일.
    """
    filtered = _filter_window(transformed_df, merge_window_days)
    filtered.createOrReplaceTempView("source_processed_events_update")
    spark.sql(
        f"""
        MERGE INTO {target_table} t
        USING source_processed_events_update s
        ON t.event_id = s.event_id
        WHEN MATCHED AND (
              (s.conversion = 1 AND t.conversion = 0)
           OR (s.click = 1 AND t.click = 0)
        ) THEN
          UPDATE SET
            t.click = s.click,
            t.conversion = s.conversion,
            t.conversion_delay_sec = s.conversion_delay_sec,
            t.updated_at = s.updated_at
        """
    )


def merge_recent(spark, transformed_df, target_table, merge_window_days):
    """combined merge (legacy) — INSERT + UPDATE 한 번에. backward compat."""
    filtered = _filter_window(transformed_df, merge_window_days)
    filtered.createOrReplaceTempView("source_processed_events")
    spark.sql(
        f"""
        MERGE INTO {target_table} t
        USING source_processed_events s
        ON t.event_id = s.event_id
        WHEN MATCHED THEN
          UPDATE SET
            t.event_date = s.event_date,
            t.uid = s.uid,
            t.campaign = s.campaign,
            t.click = s.click,
            t.conversion = s.conversion,
            t.conversion_delay_sec = s.conversion_delay_sec,
            t.cost = s.cost,
            t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN
          INSERT (
            event_id, event_date, uid, campaign,
            click, conversion, conversion_delay_sec, cost, updated_at
          ) VALUES (
            s.event_id, s.event_date, s.uid, s.campaign,
            s.click, s.conversion, s.conversion_delay_sec, s.cost, s.updated_at
          )
        """
    )


def main():
    parser = argparse.ArgumentParser(
        description="raw files (3 zone) -> processed_events Iceberg (event_id join + MERGE)"
    )
    parser.add_argument(
        "--impression-path",
        required=True,
        help="raw/impressions/ zone 경로",
    )
    parser.add_argument(
        "--click-path",
        required=True,
        help="raw/clicks/ zone 경로",
    )
    parser.add_argument(
        "--conversion-path",
        required=True,
        help="raw/conversions/ zone 경로",
    )
    parser.add_argument("--raw-format", choices=["parquet", "json"], default="parquet")
    parser.add_argument("--catalog-name", default="glue_catalog")
    parser.add_argument(
        "--warehouse",
        required=True,
        help="S3 warehouse 경로 (예: s3://<bucket>/lakehouse-lab/warehouse)",
    )
    parser.add_argument("--database", default="ad_lakehouse")
    parser.add_argument("--table", default="processed_events")
    parser.add_argument(
        "--mode",
        choices=["insert", "update", "merge", "full-refresh"],
        default="merge",
        help=(
            "insert: 새 event_id 만 INSERT (anti-join). "
            "update: 기존 event_id 의 늦게 도착한 conversion/click 만 MERGE-UPDATE. "
            "merge: 위 둘을 한 번에 (legacy). "
            "full-refresh: 전체 덮어쓰기."
        ),
    )
    parser.add_argument("--merge-window-days", type=int, default=7)
    args = parser.parse_args()

    spark = build_spark(
        "RawToProcessedIceberg",
        args.catalog_name,
        args.warehouse,
    )
    target_table = f"{args.catalog_name}.{args.database}.{args.table}"
    ensure_processed_table(spark, target_table, args.catalog_name, args.database)

    impression_df = read_zone(spark, args.impression_path, args.raw_format)
    click_df = read_zone(spark, args.click_path, args.raw_format)
    conversion_df = read_zone(spark, args.conversion_path, args.raw_format)

    processed_df = transform_raw(impression_df, click_df, conversion_df)

    if args.mode == "full-refresh":
        full_refresh(spark, processed_df, target_table)
    elif args.mode == "insert":
        insert_new_events(spark, processed_df, target_table, args.merge_window_days)
    elif args.mode == "update":
        update_late_arrivals(spark, processed_df, target_table, args.merge_window_days)
    else:
        merge_recent(spark, processed_df, target_table, args.merge_window_days)

    print(f"processed target:  {target_table}")
    print(f"impression source: {args.impression_path}")
    print(f"click source:      {args.click_path}")
    print(f"conversion source: {args.conversion_path}")


if __name__ == "__main__":
    main()
