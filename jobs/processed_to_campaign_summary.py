"""
processed_events -> campaign_summary Iceberg.
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
            'write.update.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read'
        )
        """
    )


_AGG_SOURCE_SQL = """
SELECT
  event_date AS summary_date,
  campaign,
  COUNT(*) AS impressions,
  SUM(click) AS clicks,
  SUM(conversion) AS conversions,
  SUM(cost) AS total_cost,
  current_timestamp() AS updated_at
FROM {processed_table}
WHERE event_date >= current_date() - INTERVAL {window} DAYS
GROUP BY event_date, campaign
"""


def insert_new_summary(spark, processed_table, summary_table, merge_window_days):
    """새 (summary_date, campaign) 키만 INSERT (LEFT ANTI JOIN).

    매시간 새 캠페인 / 새 일자가 나타나는 케이스를 다룬다.
    KPI 도 같이 계산해 한 번에 적재.
    """
    src = _AGG_SOURCE_SQL.format(processed_table=processed_table, window=merge_window_days)
    spark.sql(
        f"""
        INSERT INTO {summary_table}
        SELECT
          s.summary_date,
          s.campaign,
          s.impressions,
          s.clicks,
          s.conversions,
          s.total_cost,
          CASE WHEN s.impressions > 0 THEN s.clicks * 100.0 / s.impressions ELSE 0 END AS ctr,
          CASE WHEN s.clicks > 0      THEN s.conversions * 100.0 / s.clicks      ELSE 0 END AS cvr,
          CASE WHEN s.conversions > 0 THEN s.total_cost / s.conversions          ELSE NULL END AS cpa,
          s.updated_at
        FROM ({src}) s
        LEFT ANTI JOIN {summary_table} t
          ON t.summary_date = s.summary_date AND t.campaign = s.campaign
        """
    )


def update_existing_summary(spark, processed_table, summary_table, merge_window_days):
    """이미 존재하는 (summary_date, campaign) 키는 KPI 재계산해 UPDATE.

    silver 의 conversion delay update 가 gold 까지 전파되도록 윈도우 안 모든 기존 키 갱신.
    """
    src = _AGG_SOURCE_SQL.format(processed_table=processed_table, window=merge_window_days)
    spark.sql(
        f"""
        MERGE INTO {summary_table} t
        USING ({src}) s
        ON t.summary_date = s.summary_date AND t.campaign = s.campaign
        WHEN MATCHED THEN
          UPDATE SET
            t.impressions = s.impressions,
            t.clicks      = s.clicks,
            t.conversions = s.conversions,
            t.total_cost  = s.total_cost,
            t.ctr = CASE WHEN s.impressions > 0
                         THEN s.clicks * 100.0 / s.impressions ELSE 0 END,
            t.cvr = CASE WHEN s.clicks > 0
                         THEN s.conversions * 100.0 / s.clicks ELSE 0 END,
            t.cpa = CASE WHEN s.conversions > 0
                         THEN s.total_cost / s.conversions ELSE NULL END,
            t.updated_at = s.updated_at
        """
    )


def merge_summary(spark, processed_table, summary_table, merge_window_days):
    """combined merge (legacy) — INSERT + UPDATE 한 번에."""
    src = _AGG_SOURCE_SQL.format(processed_table=processed_table, window=merge_window_days)
    spark.sql(
        f"""
        MERGE INTO {summary_table} t
        USING ({src}) s
        ON t.summary_date = s.summary_date AND t.campaign = s.campaign
        WHEN MATCHED THEN
          UPDATE SET
            t.impressions = s.impressions,
            t.clicks = s.clicks,
            t.conversions = s.conversions,
            t.total_cost = s.total_cost,
            t.ctr = CASE WHEN s.impressions > 0
                         THEN s.clicks * 100.0 / s.impressions ELSE 0 END,
            t.cvr = CASE WHEN s.clicks > 0
                         THEN s.conversions * 100.0 / s.clicks ELSE 0 END,
            t.cpa = CASE WHEN s.conversions > 0
                         THEN s.total_cost / s.conversions ELSE NULL END,
            t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN
          INSERT (
            summary_date, campaign, impressions, clicks, conversions, total_cost,
            ctr, cvr, cpa, updated_at
          ) VALUES (
            s.summary_date, s.campaign, s.impressions, s.clicks, s.conversions, s.total_cost,
            CASE WHEN s.impressions > 0 THEN s.clicks * 100.0 / s.impressions ELSE 0 END,
            CASE WHEN s.clicks > 0      THEN s.conversions * 100.0 / s.clicks      ELSE 0 END,
            CASE WHEN s.conversions > 0 THEN s.total_cost / s.conversions          ELSE NULL END,
            s.updated_at
          )
        """
    )


def main():
    parser = argparse.ArgumentParser(
        description="processed_events -> campaign_summary Iceberg"
    )
    parser.add_argument("--catalog-name", default="glue_catalog")
    parser.add_argument(
        "--warehouse",
        required=True,
        help="S3 warehouse 경로 (예: s3://<bucket>/lakehouse-lab/warehouse)",
    )
    parser.add_argument("--database", default="ad_lakehouse")
    parser.add_argument("--processed-table", default="processed_events")
    parser.add_argument("--summary-table", default="campaign_summary")
    parser.add_argument("--merge-window-days", type=int, default=7)
    parser.add_argument(
        "--mode",
        choices=["insert", "update", "merge"],
        default="merge",
        help=(
            "insert: 새 (date, campaign) 키만 INSERT. "
            "update: 기존 키 KPI 재계산 UPDATE. "
            "merge: 위 둘을 한 번에 (legacy)."
        ),
    )
    args = parser.parse_args()

    spark = build_spark(
        "ProcessedToCampaignSummary",
        args.catalog_name,
        args.warehouse,
    )
    processed_table = f"{args.catalog_name}.{args.database}.{args.processed_table}"
    summary_table = f"{args.catalog_name}.{args.database}.{args.summary_table}"

    ensure_summary_table(spark, summary_table, args.catalog_name, args.database)

    if args.mode == "insert":
        insert_new_summary(spark, processed_table, summary_table, args.merge_window_days)
    elif args.mode == "update":
        update_existing_summary(spark, processed_table, summary_table, args.merge_window_days)
    else:
        merge_summary(spark, processed_table, summary_table, args.merge_window_days)

    print(f"processed source: {processed_table}")
    print(f"summary target: {summary_table}")


if __name__ == "__main__":
    main()
