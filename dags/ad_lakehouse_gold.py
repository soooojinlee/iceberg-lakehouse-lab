"""
ad_lakehouse_gold — Silver processed_events → Gold campaign_summary 의 *전체 라이프사이클*.

  insert(new keys)  ─▶  update(recompute existing)  ─▶  compact  ─▶  rewrite_deletes  ─▶  expire  ─▶  orphan(Sun)

silver 와 동일한 두-단계 패턴:
  - INSERT: 새 (summary_date, campaign) 키만 anti-join 으로 추가 (KPI 도 한 번에 계산)
  - UPDATE: 기존 키 KPI 재계산 (silver 의 conversion delay update 가 KPI 까지 전파되도록)

매시간 :30 (UTC) — silver(:00) 가 끝난 뒤 30분 buffer.
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
GOLD_WINDOW_DAYS = int(os.getenv("ICEBERG_GOLD_WINDOW_DAYS", "7"))
GOLD_TABLE = "campaign_summary"

SPARK_CONTAINER = "spark-iceberg"
SPARK_SUBMIT = "/usr/local/spark/bin/spark-submit"
GOLD_JOB = "/home/jovyan/jobs/processed_to_campaign_summary.py"
MAINT_JOB = "/home/jovyan/jobs/iceberg_maintenance.py"

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def gold_job_cmd(mode: str) -> str:
    return dedent(
        f"""
        docker exec {SPARK_CONTAINER} {SPARK_SUBMIT} {GOLD_JOB} \\
          --catalog-name {CATALOG_NAME} \\
          --warehouse {WAREHOUSE} \\
          --database {DATABASE} \\
          --merge-window-days {GOLD_WINDOW_DAYS} \\
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
          --table {GOLD_TABLE} \\
          --action {action} {extra}
        """
    ).strip()


with DAG(
    dag_id="ad_lakehouse_gold",
    description="Gold lifecycle: insert → update → compact → rewrite-deletes → expire → orphan(Sun)",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule="30 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "iceberg", "gold"],
) as dag:

    insert_new = BashOperator(
        task_id="insert_new_summary",
        bash_command=gold_job_cmd("insert"),
    )

    update_existing = BashOperator(
        task_id="update_existing_summary",
        bash_command=gold_job_cmd("update"),
    )

    compact = BashOperator(
        task_id="compact_campaign_summary",
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

    insert_new >> update_existing >> compact >> rewrite_deletes >> expire >> weekly_orphan
