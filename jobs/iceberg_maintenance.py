"""
Iceberg 매니지먼트 잡: compaction / rewrite-deletes / expire / orphan.

Iceberg가 제공하는 system procedure를 호출하는 얇은 래퍼다.
실제 SQL은 5~6회차에서 다룬 것과 동일하다.

  --action compact          ->  CALL system.rewrite_data_files
  --action rewrite-deletes  ->  CALL system.rewrite_position_delete_files (MOR 전용)
  --action expire           ->  CALL system.expire_snapshots
  --action orphan           ->  CALL system.remove_orphan_files

MOR 테이블은 매 MERGE 마다 position delete 파일이 늘어나므로,
compact 와 짝지어 rewrite-deletes 를 주기적으로 돌려야 read 비용이 떠오르지 않는다.

여러 테이블을 한 번에 처리하려면 --table 을 여러 개 지정한다.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from typing import Iterable


def _older_than_literal(older_than_days: int) -> str:
    """Iceberg CALL procedure 의 `older_than` 은 SQL expression 이 아니라 *리터럴* 이어야 한다.

    `current_timestamp() - INTERVAL N DAYS` 같은 표현식을 그대로 넣으면
    `mismatched input '(' expecting STRING` 으로 파싱 실패.
    Python 에서 미리 계산해 'YYYY-MM-DD HH:MM:SS' 형식으로 만든 뒤 TIMESTAMP 리터럴로 캐스팅.
    """
    ts = datetime.now(timezone.utc) - timedelta(days=older_than_days)
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def build_spark(app_name: str, catalog_name: str, warehouse: str):
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


def _ensure_table_exists(spark, fq_table: str) -> bool:
    try:
        spark.sql(f"DESCRIBE TABLE {fq_table}").limit(1).collect()
        return True
    except Exception as exc:  # noqa: BLE001 - 테이블 미존재는 정상 흐름의 한 갈래
        print(f"skip {fq_table}: {exc}")
        return False


def compact_table(spark, catalog_name: str, database: str, table: str, min_input_files: int) -> None:
    fq = f"{catalog_name}.{database}.{table}"
    if not _ensure_table_exists(spark, fq):
        return
    print(f"==> compact {fq} (min-input-files={min_input_files})")
    spark.sql(
        f"""
        CALL {catalog_name}.system.rewrite_data_files(
          table => '{database}.{table}',
          options => map('min-input-files', '{min_input_files}')
        )
        """
    ).show(truncate=False)


def expire_table(
    spark, catalog_name: str, database: str, table: str, retain_last: int, older_than_days: int
) -> None:
    fq = f"{catalog_name}.{database}.{table}"
    if not _ensure_table_exists(spark, fq):
        return
    older_than = _older_than_literal(older_than_days)
    print(f"==> expire {fq} (retain={retain_last}, older_than={older_than})")
    spark.sql(
        f"""
        CALL {catalog_name}.system.expire_snapshots(
          table => '{database}.{table}',
          older_than => TIMESTAMP '{older_than}',
          retain_last => {retain_last}
        )
        """
    ).show(truncate=False)


def rewrite_position_deletes(
    spark, catalog_name: str, database: str, table: str
) -> None:
    """MOR 테이블의 position delete 파일을 컴팩션한다.

    매 MERGE 마다 position-delete 파일이 누적되면 read time 에 적용해야 할 delete 가
    선형으로 늘어나 쿼리 비용이 오른다. 주기적으로 한 번에 묶어 다시 쓴다.
    """
    fq = f"{catalog_name}.{database}.{table}"
    if not _ensure_table_exists(spark, fq):
        return
    print(f"==> rewrite position deletes {fq}")
    spark.sql(
        f"""
        CALL {catalog_name}.system.rewrite_position_delete_files(
          table => '{database}.{table}'
        )
        """
    ).show(truncate=False)


def remove_orphans(
    spark, catalog_name: str, database: str, table: str, older_than_days: int
) -> None:
    fq = f"{catalog_name}.{database}.{table}"
    if not _ensure_table_exists(spark, fq):
        return
    older_than = _older_than_literal(older_than_days)
    print(f"==> orphan cleanup {fq} (older_than={older_than})")
    spark.sql(
        f"""
        CALL {catalog_name}.system.remove_orphan_files(
          table => '{database}.{table}',
          older_than => TIMESTAMP '{older_than}'
        )
        """
    ).show(truncate=False)


def run(action: str, spark, catalog_name: str, database: str, tables: Iterable[str], args) -> None:
    for table in tables:
        if action == "compact":
            compact_table(spark, catalog_name, database, table, args.min_input_files)
        elif action == "rewrite-deletes":
            rewrite_position_deletes(spark, catalog_name, database, table)
        elif action == "expire":
            expire_table(
                spark, catalog_name, database, table, args.retain_last, args.older_than_days
            )
        elif action == "orphan":
            remove_orphans(spark, catalog_name, database, table, args.older_than_days)
        else:
            raise ValueError(f"unsupported action: {action}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Iceberg maintenance (compact / expire / orphan)")
    parser.add_argument("--catalog-name", default="glue_catalog")
    parser.add_argument(
        "--warehouse",
        required=True,
        help="S3 warehouse 경로 (예: s3://<bucket>/lakehouse-lab/warehouse)",
    )
    parser.add_argument("--database", default="ad_lakehouse")
    parser.add_argument(
        "--table",
        nargs="+",
        required=True,
        help="대상 테이블 (예: --table processed_events campaign_summary)",
    )
    parser.add_argument(
        "--action",
        choices=["compact", "rewrite-deletes", "expire", "orphan"],
        required=True,
    )
    parser.add_argument("--min-input-files", type=int, default=5)
    parser.add_argument("--retain-last", type=int, default=10)
    parser.add_argument("--older-than-days", type=int, default=7)
    args = parser.parse_args()

    spark = build_spark(
        f"IcebergMaintenance[{args.action}]",
        args.catalog_name,
        args.warehouse,
    )
    run(args.action, spark, args.catalog_name, args.database, args.table, args)


if __name__ == "__main__":
    main()
