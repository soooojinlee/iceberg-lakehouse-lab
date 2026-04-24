"""
raw files -> processed_events Iceberg.

raw zone는 append-only 파일이고, 실제 테이블 관리는 processed_events부터 시작한다.
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


def read_raw(spark, raw_path, raw_format):
    df = spark.read.format(raw_format).load(raw_path)
    return df


def transform_raw(df):
    from pyspark.sql.functions import (
        col,
        current_timestamp,
        to_date,
        when,
    )

    return (
        df.withColumn("event_date", to_date(col("event_timestamp")))
        .withColumn(
            "conversion_delay_sec",
            when(
                col("conversion") == 1,
                col("conversion_timestamp") - col("timestamp"),
            ),
        )
        .withColumn("updated_at", current_timestamp())
        .select(
            col("event_id"),
            col("event_date"),
            col("uid"),
            col("campaign").cast("int"),
            col("click").cast("int"),
            col("conversion").cast("int"),
            col("conversion_delay_sec").cast("bigint"),
            col("cost").cast("double"),
            col("updated_at"),
        )
        .dropDuplicates(["event_id"])
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
    parser = argparse.ArgumentParser(description="raw files -> processed_events Iceberg")
    parser.add_argument("--raw-path", required=True)
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

    raw_df = read_raw(spark, args.raw_path, args.raw_format)
    processed_df = transform_raw(raw_df)

    if args.mode == "full-refresh":
        full_refresh(spark, processed_df, target_table)
    else:
        merge_recent(spark, processed_df, target_table, args.merge_window_days)

    print(f"processed target: {target_table}")
    print(f"source raw path: {args.raw_path}")


if __name__ == "__main__":
    main()
