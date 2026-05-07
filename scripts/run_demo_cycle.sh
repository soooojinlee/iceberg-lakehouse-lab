#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# 호스트 측 Python 은 venv 의 것을 쓴다. kafka-python-ng / faker 등이 venv 에만 있다.
VENV_PY="${VENV_PY:-${REPO_ROOT}/.venv/bin/python3}"
if [ ! -x "$VENV_PY" ]; then
  printf "\033[1;31m!!  venv python 없음: %s\033[0m\n" "$VENV_PY"
  printf "    먼저 venv 셋업: python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt\n"
  exit 1
fi

EVENTS="${EVENTS:-2000}"
SPEED="${SPEED:-500}"
DELAY_SCALE="${DELAY_SCALE:-0.0001}"
PRODUCER_DURATION_SEC="${PRODUCER_DURATION_SEC:-30}"
STREAMING_DURATION_SEC="${STREAMING_DURATION_SEC:-45}"

BUCKET="${BUCKET:-metacode-study-datalake}"
PREFIX="${PREFIX:-ad_lakehouse}"
AWS_PROFILE="${AWS_PROFILE:-iceberg-lab}"
AWS_REGION="${AWS_REGION:-ap-northeast-2}"

S3_RAW_BASE="s3a://${BUCKET}/${PREFIX}/raw"
S3_CHECKPOINT_BASE="s3a://${BUCKET}/${PREFIX}/checkpoints"
S3_WAREHOUSE="s3://${BUCKET}/${PREFIX}/warehouse"

CATALOG_NAME="glue_catalog"
DATABASE="ad_lakehouse"

AIRFLOW_HOST="${AIRFLOW_HOST:-http://localhost:8080}"

say()  { printf "\n\033[1;36m==> %s\033[0m\n" "$*"; }
warn() { printf "\033[1;33m!!  %s\033[0m\n" "$*"; }

# ---------- (1) compose 상태 ----------
say "1) docker compose 상태 확인"
required_services=(spark-iceberg kafka airflow-webserver airflow-scheduler)
missing=()
for svc in "${required_services[@]}"; do
  if ! docker ps --format '{{.Names}}' | grep -q "^${svc}$"; then
    missing+=("$svc")
  fi
done
if [ "${#missing[@]}" -gt 0 ]; then
  warn "필요한 서비스가 없습니다: ${missing[*]}"
  cat <<EOF

  docker compose up -d --build
  docker compose --profile streaming up -d
  ICEBERG_WAREHOUSE=${S3_WAREHOUSE} ICEBERG_RAW_BASE=s3://${BUCKET}/${PREFIX}/raw \\
    docker compose --profile airflow up -d --build

EOF
  exit 1
fi

# ---------- AWS 자격 ----------
say "1b) AWS 자격 확인"
aws sts get-caller-identity --profile "$AWS_PROFILE" >/dev/null
aws s3api head-bucket --bucket "$BUCKET" --profile "$AWS_PROFILE" --region "$AWS_REGION" 2>/dev/null \
  || { warn "버킷 접근 실패: $BUCKET"; exit 1; }
echo "  bucket: $BUCKET / prefix: $PREFIX / region: $AWS_REGION"

# spark-iceberg 가 docker exec 시 AWS 자격증명을 환경변수로도 받게 한다.
export AWS_ACCESS_KEY_ID
AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id --profile "$AWS_PROFILE")
export AWS_SECRET_ACCESS_KEY
AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key --profile "$AWS_PROFILE")
export AWS_DEFAULT_REGION="$AWS_REGION"

# ---------- (2) 샘플 데이터 ----------
SAMPLE_CSV="data/ad_events_sample.csv"
if [ ! -f "$SAMPLE_CSV" ]; then
  say "2) 샘플 데이터 생성 ($EVENTS events)"
  "$VENV_PY" generate_sample_data.py --events "$EVENTS"
else
  say "2) 샘플 데이터 존재 ($SAMPLE_CSV) - 생성 생략"
fi

# ---------- (3) Kafka 토픽 ----------
say "3) Kafka 토픽 생성 (이미 있으면 무시)"
docker exec kafka bash -c '
for t in ad-impressions ad-clicks ad-conversions; do
  kafka-topics --bootstrap-server localhost:9092 --create \
    --topic "$t" --partitions 3 --replication-factor 1 \
    --if-not-exists
done'

# ---------- (4) producer ----------
say "4) Kafka producer 발행 (백그라운드, 최대 ${PRODUCER_DURATION_SEC}초)"
"$VENV_PY" kafka_producer.py \
  --csv "$SAMPLE_CSV" \
  --speed "$SPEED" \
  --delay-scale "$DELAY_SCALE" \
  --max-events "$EVENTS" \
  >/tmp/lakehouse_producer.log 2>&1 &
PRODUCER_PID=$!
sleep 3
if ! kill -0 "$PRODUCER_PID" 2>/dev/null; then
  warn "producer 가 즉시 종료됨. 로그:"
  cat /tmp/lakehouse_producer.log
  exit 1
fi

# ---------- (5) streaming ----------
say "5) Spark Structured Streaming (S3 raw zone)"
run_stream() {
  local etype="$1"
  local zone="$2"
  # spark-sql-kafka 와 transitive deps 가 이미 image 의 /usr/local/spark/jars 에 들어 있어
  # --packages (Ivy resolution) 가 불필요하다. 컨테이너 재시작 시 의존성 다운로드 실패에
  # 묶이지 않게 하려는 결정.
  docker exec -d \
    -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_DEFAULT_REGION \
    spark-iceberg bash -c "
    /usr/local/spark/bin/spark-submit \
      /home/jovyan/jobs/kafka_to_raw_files.py \
      --kafka-packages '' \
      --bootstrap-servers kafka:29092 \
      --event-type ${etype} \
      --raw-path ${S3_RAW_BASE}/${zone} \
      --checkpoint-path ${S3_CHECKPOINT_BASE}/raw-${zone} \
      > /tmp/kafka_stream_${zone}.log 2>&1
  "
}

# 기존 streaming 잡 정리
docker exec spark-iceberg bash -c "pgrep -f kafka_to_raw_files.py | xargs -r kill" || true
sleep 2

run_stream impression impressions
run_stream click clicks
run_stream conversion conversions

say "5b) ${STREAMING_DURATION_SEC}초 동안 raw zone 적재 대기"
sleep "$STREAMING_DURATION_SEC"

if kill -0 "$PRODUCER_PID" 2>/dev/null; then
  kill "$PRODUCER_PID" 2>/dev/null || true
  wait "$PRODUCER_PID" 2>/dev/null || true
fi

say "5c) S3 raw zone 객체 카운트"
for zone in impressions clicks conversions; do
  n=$(aws s3 ls "s3://${BUCKET}/${PREFIX}/raw/${zone}/" --recursive \
        --profile "$AWS_PROFILE" --region "$AWS_REGION" 2>/dev/null | wc -l | tr -d ' ')
  echo "  raw/$zone : $n S3 objects"
done

# streaming 종료 (시연 자리에선 한 번 채우면 충분)
docker exec spark-iceberg bash -c "pgrep -f kafka_to_raw_files.py | xargs -r kill" || true

# ---------- (6) Airflow trigger ----------
trigger_dag() {
  local dag_id="$1"
  say "6) Airflow trigger: $dag_id"
  docker exec airflow-scheduler airflow dags unpause "$dag_id" >/dev/null 2>&1 || true
  docker exec airflow-scheduler airflow dags trigger "$dag_id"
}

wait_dag() {
  local dag_id="$1"
  local max_wait="${2:-300}"
  local elapsed=0
  while [ "$elapsed" -lt "$max_wait" ]; do
    # Airflow CLI 가 plugins INFO 라인을 stdout 으로 출력하므로 grep 로 JSON 라인만 추린다.
    local raw_json
    # JSON 출력은 '[{' 로 시작하는 단일 라인. Airflow 의 INFO 로그는 '[YYYY-...]' 로 시작하므로 구분 가능.
    raw_json=$(docker exec airflow-scheduler airflow dags list-runs -d "$dag_id" --output json 2>/dev/null \
               | { grep -E '^\[\{' || true; })
    if [ -z "$raw_json" ]; then
      raw_json=$(docker exec airflow-scheduler airflow dags list-runs -d "$dag_id" --output json 2>/dev/null \
                 | { grep -E '^\[\]' || true; })
    fi
    local state
    state=$(printf '%s' "$raw_json" | "$VENV_PY" -c "
import json, sys
try:
    data = json.loads(sys.stdin.read() or '[]')
    print(data[0]['state'] if data else '')
except Exception:
    print('')
" 2>/dev/null)
    case "$state" in
      success) echo "  $dag_id: success"; return 0 ;;
      failed)  warn "$dag_id: failed"; return 1 ;;
      *) sleep 5; elapsed=$((elapsed+5)) ;;
    esac
  done
  warn "$dag_id: timeout after ${max_wait}s"
  return 1
}

trigger_dag ad_lakehouse_silver
wait_dag ad_lakehouse_silver 600

trigger_dag ad_lakehouse_gold
wait_dag ad_lakehouse_gold 600

# 각 DAG 이 maintenance 까지 다 들고 있으므로 별도 maintenance trigger 는 없다.

# ---------- (8) 결과 요약 ----------
say "8) Glue + Iceberg 결과 요약"
docker exec \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_DEFAULT_REGION \
  spark-iceberg /usr/local/spark/bin/spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.${CATALOG_NAME}=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.${CATALOG_NAME}.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
  --conf spark.sql.catalog.${CATALOG_NAME}.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.${CATALOG_NAME}.warehouse=${S3_WAREHOUSE} \
  -e "
    SELECT 'processed_events' AS table_name, count(*) AS row_count FROM ${CATALOG_NAME}.${DATABASE}.processed_events
    UNION ALL
    SELECT 'campaign_summary' AS table_name, count(*) AS row_count FROM ${CATALOG_NAME}.${DATABASE}.campaign_summary;
    SELECT * FROM ${CATALOG_NAME}.${DATABASE}.processed_events.snapshots ORDER BY committed_at DESC LIMIT 3;
    SELECT * FROM ${CATALOG_NAME}.${DATABASE}.campaign_summary.snapshots ORDER BY committed_at DESC LIMIT 3;
  " || warn "spark-sql 결과 조회 실패 - 컨테이너 / 자격 / 테이블 존재 여부 확인"

say "완료. Airflow Web UI: $AIRFLOW_HOST  (admin / admin)"
echo "  Glue: aws glue get-tables --database-name ${DATABASE} --profile ${AWS_PROFILE} --region ${AWS_REGION}"
