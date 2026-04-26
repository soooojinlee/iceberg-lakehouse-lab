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


def build_spark(app_name, catalog_mode, catalog_name, warehouse):
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
    )

    if catalog_mode == "glue":
        builder = (
            builder.config(
                f"spark.sql.catalog.{catalog_name}.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog",
            )
            .config(
                f"spark.sql.catalog.{catalog_name}.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO",
            )
            .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse)
        )
    else:
        builder = (
            builder.config(f"spark.sql.catalog.{catalog_name}.type", "hadoop")
            .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse)
        )

    return builder.getOrCreate()


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
            'write.update.mode' = 'copy-on-write',
            'write.merge.mode' = 'copy-on-write',
            'write.delete.mode' = 'copy-on-write',
            'write.target-file-size-bytes' = '134217728'
        )
        """
    )


def read_zone(spark, path, raw_format):
    return spark.read.format(raw_format).load(path)


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


def merge_recent(spark, transformed_df, target_table, merge_window_days):
    filtered = transformed_df.filter(
        f"event_date >= current_date() - INTERVAL {merge_window_days} DAYS"
    )
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
            event_id,
            event_date,
            uid,
            campaign,
            click,
            conversion,
            conversion_delay_sec,
            cost,
            updated_at
          )
          VALUES (
            s.event_id,
            s.event_date,
            s.uid,
            s.campaign,
            s.click,
            s.conversion,
            s.conversion_delay_sec,
            s.cost,
            s.updated_at
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
    parser.add_argument("--catalog-mode", choices=["local", "glue"], default="local")
    parser.add_argument("--catalog-name", default="local")
    parser.add_argument("--warehouse", default="/home/jovyan/warehouse")
    parser.add_argument("--database", default="ad_lakehouse")
    parser.add_argument("--table", default="processed_events")
    parser.add_argument("--mode", choices=["merge", "full-refresh"], default="merge")
    parser.add_argument("--merge-window-days", type=int, default=7)
    args = parser.parse_args()

    spark = build_spark(
        "RawToProcessedIceberg",
        args.catalog_mode,
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
    else:
        merge_recent(spark, processed_df, target_table, args.merge_window_days)

    print(f"processed target:  {target_table}")
    print(f"impression source: {args.impression_path}")
    print(f"click source:      {args.click_path}")
    print(f"conversion source: {args.conversion_path}")


if __name__ == "__main__":
    main()
