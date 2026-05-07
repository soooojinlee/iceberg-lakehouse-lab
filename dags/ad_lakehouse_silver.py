"""
ad_lakehouse_silver — Bronze raw zones → Silver processed_events 의 *전체 라이프사이클*.

  check_raw  ─▶  insert  ─▶  update(late)  ─▶  compact  ─▶  rewrite_deletes  ─▶  expire  ─▶  orphan(Sun)

INSERT 와 MERGE 를 별도 task 로 분리하는 이유:
  - 정상 흐름의 대부분은 새 event_id 의 INSERT (cheap, write-heavy 정상 경로)
  - 늦게 도착하는 conversion / click 의 UPDATE 만 MERGE-WHEN-MATCHED (드물지만 무거움)
  - 5회차의 "MERGE 두 분기" 가 시연 자리에서 *서로 다른 task* 로 보임

매시간 사이클마다 maintenance 까지 함께 — 작은 lab 환경에서 한 사이클이 곧 *완성된 라이프사이클*.
운영에서는 compaction 주기를 분리하지만, 시연 의도상 한 DAG 안에 모두 둔다.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule


CATALOG_NAME = os.getenv("ICEBERG_CATALOG_NAME", "glue_catalog")
WAREHOUSE = os.environ["ICEBERG_WAREHOUSE"]
DATABASE = os.getenv("ICEBERG_DATABASE", "ad_lakehouse")
RAW_BASE = os.environ["ICEBERG_RAW_BASE"]
MERGE_WINDOW_DAYS = int(os.getenv("ICEBERG_MERGE_WINDOW_DAYS", "7"))
SILVER_TABLE = "processed_events"

SPARK_CONTAINER = "spark-iceberg"
SPARK_SUBMIT = "/usr/local/spark/bin/spark-submit"
SILVER_JOB = "/home/jovyan/jobs/raw_to_processed_iceberg.py"
MAINT_JOB = "/home/jovyan/jobs/iceberg_maintenance.py"

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def check_raw_cmd() -> str:
    bucket_prefix = RAW_BASE.replace("s3a://", "").replace("s3://", "")
    bucket, _, prefix = bucket_prefix.partition("/")
    return dedent(
        f"""
        set -e
        docker exec {SPARK_CONTAINER} python3 - <<'PYCHK'
import sys, boto3
s3 = boto3.client("s3")
bucket = "{bucket}"
prefix_base = "{prefix}"
empty = []
for z in ["impressions", "clicks", "conversions"]:
    p = f"{{prefix_base.rstrip('/')}}/{{z}}/" if prefix_base else f"{{z}}/"
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=p, MaxKeys=1)
    n = resp.get("KeyCount", 0)
    print(f"raw/{{z}} keys: {{n}}")
    if n == 0:
        empty.append(z)
if empty:
    print(f"raw zones empty: {{empty}} - skip silver insert")
    sys.exit(99)
PYCHK
        """
    ).strip()


def silver_job_cmd(mode: str) -> str:
    return dedent(
        f"""
        docker exec {SPARK_CONTAINER} {SPARK_SUBMIT} {SILVER_JOB} \\
          --catalog-name {CATALOG_NAME} \\
          --warehouse {WAREHOUSE} \\
          --database {DATABASE} \\
          --impression-path {RAW_BASE}/impressions \\
          --click-path {RAW_BASE}/clicks \\
          --conversion-path {RAW_BASE}/conversions \\
          --merge-window-days {MERGE_WINDOW_DAYS} \\
          --mode {mode}
        """
    ).strip()


def maint_cmd(action: str, extra: str = "") -> str:
    return dedent(
        f"""
        docker exec {SPARK_CONTAINER} {SPARK_SUBMIT} {MAINT_JOB} \\
          --catalog-name {CATALOG_NAME} \\
          --warehouse {WAREHOUSE} \\
          --database {DATABASE} \\
          --table {SILVER_TABLE} \\
          --action {action} {extra}
        """
    ).strip()


with DAG(
    dag_id="ad_lakehouse_silver",
    description="Silver lifecycle: insert → update → compact → rewrite-deletes → expire → orphan(Sun)",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "iceberg", "silver"],
) as dag:

    check = BashOperator(
        task_id="check_raw_zones_have_data",
        bash_command=check_raw_cmd(),
    )

    insert_new = BashOperator(
        task_id="insert_new_events",
        bash_command=silver_job_cmd("insert"),
    )

    update_late = BashOperator(
        task_id="update_late_arrivals",
        bash_command=silver_job_cmd("update"),
    )

    compact = BashOperator(
        task_id="compact_processed_events",
        bash_command=maint_cmd("compact", "--min-input-files 5"),
    )

    rewrite_deletes = BashOperator(
        task_id="rewrite_position_deletes",
        bash_command=maint_cmd("rewrite-deletes"),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    expire = BashOperator(
        task_id="expire_snapshots",
        bash_command=maint_cmd("expire", "--retain-last 10 --older-than-days 7"),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # orphan 은 비용이 커서 일요일에만 실제로 동작.
    weekly_orphan = BashOperator(
        task_id="remove_orphan_files_weekly",
        bash_command=(
            'if [ "$(date -u +%u)" = "7" ]; then\n'
            f"  {maint_cmd('orphan', '--older-than-days 7')}\n"
            "else\n"
            '  echo "skip: orphan cleanup only runs on Sunday (UTC)";\n'
            "fi"
        ),
    )

    check >> insert_new >> update_late >> compact >> rewrite_deletes >> expire >> weekly_orphan
