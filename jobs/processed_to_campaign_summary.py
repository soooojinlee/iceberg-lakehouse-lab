"""
processed_events -> campaign_summary Iceberg.
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


def ensure_summary_table(spark, target_table, catalog_name, database):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{database}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            summary_date DATE,
            campaign INT,
            impressions BIGINT,
            clicks BIGINT,
            conversions BIGINT,
            total_cost DOUBLE,
            ctr DOUBLE,
            cvr DOUBLE,
            cpa DOUBLE,
            updated_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (summary_date)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.update.mode' = 'copy-on-write',
            'write.merge.mode' = 'copy-on-write',
            'write.delete.mode' = 'copy-on-write'
        )
        """
    )


def merge_summary(spark, processed_table, summary_table, merge_window_days):
    spark.sql(
        f"""
        MERGE INTO {summary_table} t
        USING (
          SELECT
            event_date AS summary_date,
            campaign,
            COUNT(*) AS impressions,
            SUM(click) AS clicks,
            SUM(conversion) AS conversions,
            SUM(cost) AS total_cost,
            current_timestamp() AS updated_at
          FROM {processed_table}
          WHERE event_date >= current_date() - INTERVAL {merge_window_days} DAYS
          GROUP BY event_date, campaign
        ) s
        ON t.summary_date = s.summary_date AND t.campaign = s.campaign
        WHEN MATCHED THEN
          UPDATE SET
            t.impressions = s.impressions,
            t.clicks = s.clicks,
            t.conversions = s.conversions,
            t.total_cost = s.total_cost,
            t.ctr = CASE WHEN s.impressions > 0
                         THEN s.clicks * 100.0 / s.impressions
                         ELSE 0 END,
            t.cvr = CASE WHEN s.clicks > 0
                         THEN s.conversions * 100.0 / s.clicks
                         ELSE 0 END,
            t.cpa = CASE WHEN s.conversions > 0
                         THEN s.total_cost / s.conversions
                         ELSE NULL END,
            t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN
          INSERT (
            summary_date,
            campaign,
            impressions,
            clicks,
            conversions,
            total_cost,
            ctr,
            cvr,
            cpa,
            updated_at
          )
          VALUES (
            s.summary_date,
            s.campaign,
            s.impressions,
            s.clicks,
            s.conversions,
            s.total_cost,
            CASE WHEN s.impressions > 0 THEN s.clicks * 100.0 / s.impressions ELSE 0 END,
            CASE WHEN s.clicks > 0 THEN s.conversions * 100.0 / s.clicks ELSE 0 END,
            CASE WHEN s.conversions > 0 THEN s.total_cost / s.conversions ELSE NULL END,
            s.updated_at
          )
        """
    )


def main():
    parser = argparse.ArgumentParser(
        description="processed_events -> campaign_summary Iceberg"
    )
    parser.add_argument("--catalog-mode", choices=["local", "glue"], default="local")
    parser.add_argument("--catalog-name", default="local")
    parser.add_argument("--warehouse", default="/home/jovyan/warehouse")
    parser.add_argument("--database", default="ad_lakehouse")
    parser.add_argument("--processed-table", default="processed_events")
    parser.add_argument("--summary-table", default="campaign_summary")
    parser.add_argument("--merge-window-days", type=int, default=7)
    args = parser.parse_args()

    spark = build_spark(
        "ProcessedToCampaignSummary",
        args.catalog_mode,
        args.catalog_name,
        args.warehouse,
    )
    processed_table = f"{args.catalog_name}.{args.database}.{args.processed_table}"
    summary_table = f"{args.catalog_name}.{args.database}.{args.summary_table}"

    ensure_summary_table(spark, summary_table, args.catalog_name, args.database)
    merge_summary(spark, processed_table, summary_table, args.merge_window_days)

    print(f"processed source: {processed_table}")
    print(f"summary target: {summary_table}")


if __name__ == "__main__":
    main()
