# 광고 플랫폼 Lakehouse 실전 설계 - 실습 환경

> **8회차 최종 프로젝트 안내**: [`docs/final-project-guide.md`](docs/final-project-guide.md)

이 repo는 강의 개요의 `Raw -> Processing -> Summary` 흐름은 유지하되, 저장 계층은 다음처럼 가져간다.

- `raw`: Iceberg 테이블이 아니라 **이벤트 타입별 plain parquet append zone** (impressions / clicks / conversions)
- `processed_events`: **Iceberg** (event_id 기준 join + MERGE)
- `campaign_summary`: **Iceberg**

즉, 기본 구조는 아래와 같다.

```text
Criteo CSV
  -> Kafka (3 토픽: ad-impressions, ad-clicks, ad-conversions)
  -> Spark Structured Streaming (event-type 별 3개 잡)
  -> raw zones on S3 or local
       raw/impressions/   raw/clicks/   raw/conversions/
  -> Spark batch / incremental merge (3 zone -> event_id join)
  -> processed_events (Iceberg)
  -> campaign_summary (Iceberg)
```

이 구조는 강의 2회차의 Medallion 분리와 4회차의 `Raw / Processing / Summary` 구분에 정렬된다. bronze는 source-of-truth append-only 파일 zone, silver(`processed_events`)가 event_id 기준으로 조립자 역할을 한다.

## 사전 준비

- Docker Desktop
- Python 3.10+
- AWS 계정과 버킷
  S3 / Glue / Athena 연동을 쓰는 경우 필요
- 로컬만 쓸 경우 AWS는 선택

## 디렉토리 구조

```text
iceberg-lakehouse-lab/
├── docker-compose.yml
├── Dockerfile
├── prepare_criteo_data.py
├── generate_sample_data.py
├── split_events.py
├── kafka_producer.py
├── jobs/
│   ├── kafka_to_raw_files.py
│   ├── raw_to_processed_iceberg.py
│   └── processed_to_campaign_summary.py
├── data/
│   ├── ad_events.csv
│   ├── ad_events_sample.csv
│   └── ad_events_batch2.csv
└── notebooks/
```

## 0단계: 로컬 Python 가상환경 설정

`kafka_producer.py`, `generate_sample_data.py`, `prepare_criteo_data.py`는 호스트에서 돌린다. 시스템 Python을 오염시키지 않도록 repo 전용 venv를 만든다.

```bash
cd iceberg-lakehouse-lab
python3 -m venv .venv
source .venv/bin/activate   # Windows (PowerShell): .venv\Scripts\Activate.ps1

pip install --upgrade pip
pip install -r requirements.txt
```

`requirements.txt`는 실습 실행에 필요한 최소 세트만 담고 있다.
- `kafka-python-ng` — `kafka_producer.py`가 CSV를 Kafka로 발행할 때 사용
- `pyspark` — 호스트에서 바로 Spark를 쓸 일은 없지만 로컬 자동완성/린트용으로 들어있음 (컨테이너 안 Spark가 실제 실행 엔진)

확인.

```bash
python -c "import kafka; print(kafka.__version__)"
python -c "import pyspark; print(pyspark.__version__)"
```

세션이 바뀌면 `source .venv/bin/activate`를 다시 해야 한다. VSCode / PyCharm에서는 인터프리터를 `./.venv/bin/python`으로 설정해두면 매번 activate 안 해도 된다.

종료는 `deactivate`.

## 1단계: 데이터 준비

### 방법 A: Criteo 실제 데이터 사용

이 repo에는 Criteo 원본 파일이 포함돼 있지 않다. 직접 받아서 `data/` 아래에 둔다.

다운로드 경로 (택1):
- 공식: https://ailab.criteo.com/criteo-attribution-modeling-bidding-dataset/ (신청 폼 후 zip)
- Hugging Face 미러: https://huggingface.co/datasets/criteo/criteo-attribution-dataset (`criteo_attribution_dataset.tsv.gz`, 653MB)
- Kaggle 미러: https://www.kaggle.com/datasets/sharatsachin/criteo-attribution-modeling

라이선스는 CC BY-NC-SA 4.0 이며, 16.4M rows / 30일 / 700여 캠페인 규모의 실제 광고 attribution 데이터다.

권장 규모:
- 기본 실습: `100만건`
- 빠른 로컬 테스트: `10만건`
- 성능 비교: `300만~500만건`
- 풀스케일 도전: `1600만건 전체`

```bash
# 내려받은 파일을 data/ 아래에 두고
python prepare_criteo_data.py --input ./data/criteo_attribution_dataset.tsv.gz

# 10만건만
python prepare_criteo_data.py --input ./data/criteo_attribution_dataset.tsv.gz --sample 100000
```

`prepare_criteo_data.py`는 Criteo의 상대 timestamp를 실습용 절대시간으로 rebasing 한다.

### 방법 B: 합성 데이터 생성

```bash
python generate_sample_data.py
python generate_sample_data.py --events 1000 --output ./data
```

## 2단계: Docker 환경 시작

```bash
docker compose up -d --build
docker compose --profile streaming up -d
```

접속:
- Jupyter Notebook: `http://localhost:8888`
- Kafka UI: `http://localhost:8090`

## 3단계: Kafka로 이벤트 발행

producer는 항상 3 토픽으로 분리 발행한다. CSV 한 행은 impression(즉시) → click(수 초 후, 합성 지연) → conversion(수 시간 후, `--delay-scale`로 축소) 순으로 시점이 분리되어 흘러간다.

```bash
# 토픽 사전 생성 (1회만)
python kafka_producer.py --create-topics --csv ./data/ad_events_sample.csv --max-events 0

# 전체 발행
python kafka_producer.py --csv ./data/ad_events.csv

# 빠른 테스트
python kafka_producer.py --csv ./data/ad_events_sample.csv --speed 500

# 4회차 MERGE INTO 실습용: 전환 지연을 더 짧게 압축
python kafka_producer.py --csv ./data/ad_events_sample.csv --speed 500 --delay-scale 0.0001
```

발행 토픽:

| 토픽 | 발행 시점 | payload 핵심 필드 |
|---|---|---|
| `ad-impressions` | 즉시 | `event_id`, `timestamp`, `uid`, `campaign`, `cost` |
| `ad-clicks` | impression 후 1~30초 (스피드 배율 적용) | `event_id`, `timestamp`, `impression_timestamp`, `uid`, `campaign` |
| `ad-conversions` | `(conversion_ts - impression_ts) × delay-scale` 후 | `event_id`, `timestamp`, `impression_timestamp`, `uid`, `campaign`, `conversion_delay_sec` |

세 토픽 모두 `event_id`를 같이 실어 보내므로 silver layer가 join 으로 한 row를 조립한다. conversion이 늦게 도착해도 `event_id`만 있으면 silver의 MERGE INTO에서 기존 row를 자연스럽게 UPDATE 한다.

## 4단계: Spark가 Kafka를 읽고 raw 파일로 적재

이 단계는 Kafka 메시지를 바로 Iceberg에 쓰지 않고, event-type 별 append-only raw zone에 plain parquet으로 쌓는다. **한 spark-submit이 한 토픽 → 한 zone** 을 처리하므로 3개의 streaming 잡을 띄운다.

### 로컬 예시 (3개 잡 각각 백그라운드)

```bash
# impression
docker compose exec -d spark-iceberg \
  /usr/local/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
    /home/jovyan/jobs/kafka_to_raw_files.py \
    --kafka-packages "" \
    --bootstrap-servers kafka:29092 \
    --event-type impression \
    --raw-path /home/jovyan/warehouse/raw/impressions \
    --checkpoint-path /home/jovyan/warehouse/checkpoints/raw-impressions

# click
docker compose exec -d spark-iceberg \
  /usr/local/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
    /home/jovyan/jobs/kafka_to_raw_files.py \
    --kafka-packages "" \
    --bootstrap-servers kafka:29092 \
    --event-type click \
    --raw-path /home/jovyan/warehouse/raw/clicks \
    --checkpoint-path /home/jovyan/warehouse/checkpoints/raw-clicks

# conversion
docker compose exec -d spark-iceberg \
  /usr/local/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
    /home/jovyan/jobs/kafka_to_raw_files.py \
    --kafka-packages "" \
    --bootstrap-servers kafka:29092 \
    --event-type conversion \
    --raw-path /home/jovyan/warehouse/raw/conversions \
    --checkpoint-path /home/jovyan/warehouse/checkpoints/raw-conversions
```

### S3 예시 (impression 한 개만)

나머지 click / conversion 도 동일 패턴으로 `--event-type` 만 바꿔 띄운다.

```bash
docker compose exec -d spark-iceberg \
  /usr/local/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
    /home/jovyan/jobs/kafka_to_raw_files.py \
    --kafka-packages "" \
    --bootstrap-servers kafka:29092 \
    --event-type impression \
    --raw-path s3://<your-bucket>/raw/impressions \
    --checkpoint-path s3://<your-bucket>/checkpoints/raw-impressions
```

각 zone은 자체 schema의 plain parquet이며 `raw_date`, `raw_hour` (ingest 시점) 기준으로 파티셔닝된다. `--topic` 을 생략하면 `--event-type` 에 맞춰 `ad-impressions` / `ad-clicks` / `ad-conversions` 가 자동 선택된다.

## 5단계: raw files (3 zone) -> processed_events Iceberg

`processed_events`부터 Iceberg를 적용한다. silver 잡은 3 zone을 모두 읽어 `event_id` 기준으로 join 한 뒤 MERGE INTO 한다.

### 로컬 카탈로그 예시

```bash
docker compose exec spark-iceberg \
  /usr/local/spark/bin/spark-submit \
    /home/jovyan/jobs/raw_to_processed_iceberg.py \
    --catalog-mode local \
    --catalog-name local \
    --warehouse /home/jovyan/warehouse \
    --impression-path /home/jovyan/warehouse/raw/impressions \
    --click-path /home/jovyan/warehouse/raw/clicks \
    --conversion-path /home/jovyan/warehouse/raw/conversions
```

### Glue Catalog + S3 예시

```bash
docker compose exec spark-iceberg \
  /usr/local/spark/bin/spark-submit \
    /home/jovyan/jobs/raw_to_processed_iceberg.py \
    --catalog-mode glue \
    --catalog-name glue_catalog \
    --warehouse s3://<your-bucket>/warehouse \
    --impression-path s3://<your-bucket>/raw/impressions \
    --click-path s3://<your-bucket>/raw/clicks \
    --conversion-path s3://<your-bucket>/raw/conversions
```

기본 동작은 최근 7일 윈도우를 읽어 `processed_events`에 MERGE 한다 (`--mode full-refresh` 로 전량 덮어쓰기 가능).

### 5-1: MERGE INTO 실습 흐름 (4회차)

multi-topic 발행 구조에서는 conversion 이벤트가 impression / click 보다 늦게 `raw/conversions/` 에 도착한다. silver 잡을 두 번 돌리면 자연스럽게 1차 = INSERT 위주, 2차 = UPDATE 위주의 분기를 모두 관찰할 수 있다.

```bash
# 1) producer 띄우기 (지연 압축, 백그라운드 실행)
python kafka_producer.py \
  --csv ./data/ad_events_sample.csv \
  --speed 500 --delay-scale 0.0001
```

producer 로그에 `Conversion: ... -> ad-conversions` 카운트가 천천히 올라가면 conversion 이벤트가 늦게 도착하고 있다는 뜻이다. 이 동안 impression / click은 이미 `raw/impressions/`, `raw/clicks/` 에 쌓여 있다.

```bash
# 2) conversion이 아직 충분히 도착하지 않은 시점에 1차 MERGE
docker compose exec spark-iceberg \
  /usr/local/spark/bin/spark-submit \
    /home/jovyan/jobs/raw_to_processed_iceberg.py \
    --catalog-mode local --catalog-name local \
    --warehouse /home/jovyan/warehouse \
    --impression-path /home/jovyan/warehouse/raw/impressions \
    --click-path /home/jovyan/warehouse/raw/clicks \
    --conversion-path /home/jovyan/warehouse/raw/conversions
```

이 시점에서 target 테이블이 비어 있고 conversion zone도 비교적 비어 있으므로, MERGE source의 거의 모든 행이 `conversion=0` 으로 `WHEN NOT MATCHED THEN INSERT` 분기를 탄다. 결과 확인:

```sql
-- spark-iceberg 컨테이너 내부 spark-sql 또는 노트북에서
SELECT conversion, COUNT(*) FROM local.ad_lakehouse.processed_events GROUP BY conversion;
SELECT * FROM local.ad_lakehouse.processed_events.snapshots ORDER BY committed_at DESC LIMIT 3;
```

```bash
# 3) conversion 이 충분히 더 도착한 뒤 2차 MERGE
docker compose exec spark-iceberg \
  /usr/local/spark/bin/spark-submit \
    /home/jovyan/jobs/raw_to_processed_iceberg.py \
    --catalog-mode local --catalog-name local \
    --warehouse /home/jovyan/warehouse \
    --impression-path /home/jovyan/warehouse/raw/impressions \
    --click-path /home/jovyan/warehouse/raw/clicks \
    --conversion-path /home/jovyan/warehouse/raw/conversions
```

2차 source에는 새로 도착한 conversion 행들이 join 단계에서 합류해 `conversion=1, conversion_delay_sec=...` 으로 바뀐다. 동일 `event_id`가 이미 target에 있으므로 `WHEN MATCHED THEN UPDATE` 분기가 발화되어 `conversion`, `conversion_delay_sec`, `updated_at` 이 갱신된다.

```sql
SELECT conversion, COUNT(*) FROM local.ad_lakehouse.processed_events GROUP BY conversion;
SELECT * FROM local.ad_lakehouse.processed_events.snapshots ORDER BY committed_at DESC LIMIT 5;
SELECT operation, summary FROM local.ad_lakehouse.processed_events.history ORDER BY made_current_at DESC LIMIT 5;
```

snapshot 수가 늘고, 1차 vs 2차의 `conversion=1` 행 수가 달라지면 MERGE의 UPDATE 분기가 실제로 동작한 것이다.

## 6단계: processed_events -> campaign_summary Iceberg

```bash
docker compose exec spark-iceberg \
  /usr/local/spark/bin/spark-submit \
    /home/jovyan/jobs/processed_to_campaign_summary.py \
    --catalog-mode glue \
    --catalog-name glue_catalog \
    --warehouse s3://<your-bucket>/warehouse
```

기본 동작은 최근 7일을 재집계해 `campaign_summary`에 MERGE 한다.

## 7단계: Airflow로 매시간 / 매일 자동화

5/6단계의 `spark-submit`을 손으로 돌리는 대신, Airflow가 매시간 MERGE / 매일 maintenance를 호출하게 한다.
DAG 안의 task는 결국 README 5/6단계와 **같은 명령**을 `docker exec spark-iceberg ...` 형태로 부른다.
즉 자동화 전후의 "코드"는 다르지 않고, 그것을 *누가 언제 호출하느냐*만 바뀐다.

### 구성

| 컨테이너 | 역할 |
|---|---|
| `airflow-postgres` | Airflow 메타데이터 DB |
| `airflow-init` | 최초 1회 DB migrate + admin 사용자 생성 |
| `airflow-webserver` | Web UI (`http://localhost:8080`, admin / admin) |
| `airflow-scheduler` | DAG 스케줄링 / 실행 |

빌드 정의:
- `Dockerfile.airflow` — `apache/airflow:2.11.x` 기반에 `docker` CLI 추가 (BashOperator가 `docker exec` 호출용)
- `dags/ad_lakehouse_silver_merge.py` — 매시간 :00, raw 3 zone → `processed_events` (raw 비어있으면 가드)
- `dags/ad_lakehouse_gold_aggregation.py` — 매시간 :30 (silver 뒤), `processed_events` → `campaign_summary`
- `dags/ad_lakehouse_daily_maintenance.py` — 매일 새벽 3시(UTC), compact → rewrite-deletes → expire → (일요일) orphan
- `jobs/iceberg_maintenance.py` — `rewrite_data_files` / `rewrite_position_delete_files` / `expire_snapshots` / `remove_orphan_files` 래퍼

### 테이블 모드: Merge-on-Read (MOR)

`processed_events`, `campaign_summary` 둘 다 MOR 로 만들어진다.

```
TBLPROPERTIES (
  'format-version' = '2',
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode'  = 'merge-on-read',
  'write.delete.mode' = 'merge-on-read'
)
```

매시간 MERGE 워크로드에서 COW 는 행 한 줄 변경에도 데이터 파일 전체를 다시 쓰므로
write amplification 이 크다. MOR 은 position delete 파일을 추가하고 데이터 파일은
재사용해 commit latency 가 짧아진다. 단, read 시 delete 파일을 머지해야 하므로
**MOR + 정기 compaction (`rewrite_data_files`) + delete 파일 재작성 (`rewrite_position_delete_files`)
이 한 묶음**이며, daily maintenance DAG 이 그 역할을 맡는다.

### 실행

```bash
# 0) spark-iceberg + Kafka + Airflow 까지 같이 띄운다
docker compose up -d --build
docker compose --profile streaming up -d
docker compose --profile airflow up -d --build

# 컨테이너 상태 확인
docker compose ps
```

Web UI 접속:
- Airflow: `http://localhost:8080` (admin / admin)
- Spark UI: `http://localhost:4040` (job 실행 중에만)
- Jupyter: `http://localhost:8888`
- Kafka UI: `http://localhost:8090`

### 카탈로그 모드 전환

기본은 `local`(hadoop catalog, 자체 완결 데모) 이다. Glue + S3 모드로 흐름을 보여주려면 호스트 셸에서 환경 변수만 바꿔서 Airflow를 다시 띄우면 된다.

```bash
export ICEBERG_CATALOG_MODE=glue
export ICEBERG_CATALOG_NAME=glue_catalog
export ICEBERG_WAREHOUSE=s3://<your-bucket>/warehouse
export ICEBERG_RAW_BASE=s3://<your-bucket>/raw

docker compose --profile airflow up -d --force-recreate airflow-webserver airflow-scheduler
```

`spark-iceberg` 컨테이너는 호스트의 `~/.aws`를 마운트하므로 별도 자격증명 주입은 필요 없다.

### DAG 시연 흐름

```
[Airflow Web UI]
   │
   ├─ ad_lakehouse_silver_merge       schedule: 0 * * * *
   │     ├─ check_raw_zones_have_data
   │     └─ merge_raw_to_processed    (docker exec → raw_to_processed_iceberg.py)
   │
   ├─ ad_lakehouse_gold_aggregation   schedule: 30 * * * *
   │     └─ aggregate_processed_to_summary  (docker exec → processed_to_campaign_summary.py)
   │
   └─ ad_lakehouse_daily_maintenance  schedule: 0 3 * * *
         ├─ compact_processed_events
         ├─ compact_campaign_summary
         ├─ rewrite_position_deletes_all   (MOR delete 파일 정리)
         ├─ expire_snapshots_all
         └─ remove_orphan_files_weekly      (UTC 일요일에만 실제 동작)
```

silver(:00) 와 gold(:30) 는 30분 오프셋만 가진 독립 DAG. silver 가 늦어도 gold 의
7일 윈도우 재집계가 다음 사이클에 자연 회복하므로 cross-DAG sensor 없이도 안전하다.

### 한 사이클을 한 번에 — 부트스트랩 스크립트

시연 자리에서 Kafka 발행 → streaming → silver/gold/maintenance 까지 한 번에 돌리는 헬퍼.

```bash
# spark + kafka + airflow 가 모두 떠 있는 상태에서
./scripts/run_demo_cycle.sh

# 옵션
EVENTS=2000 SPEED=500 ./scripts/run_demo_cycle.sh
```

스크립트가 하는 일:
1. 필요한 컨테이너가 떠 있는지 확인
2. 샘플 데이터 생성 (없으면)
3. Kafka 토픽 생성 + producer 짧게 발행
4. 3 zone streaming 잡 띄워 raw 적재
5. `silver_merge` → `gold_aggregation` → `daily_maintenance` 순차 trigger + 완료 대기
6. snapshot 카운트 요약 출력

### 시연 시나리오 (수동)

1. Kafka producer를 띄워 raw zone에 이벤트가 쌓이는 상태를 만든다 (3단계 + 4단계).
2. Airflow Web UI에서 `ad_lakehouse_silver_merge` 를 manual trigger → Graph view에서 가드 → MERGE 흐름 확인.
3. 이어서 `ad_lakehouse_gold_aggregation` 을 trigger → `campaign_summary` snapshot 갱신 확인.
4. spark-iceberg 컨테이너의 Spark UI(`http://localhost:4040`)에서 실제 잡 동작 확인.
5. `processed_events` / `campaign_summary` 의 snapshot 수가 늘어나는 것을 확인.
6. `ad_lakehouse_daily_maintenance` 를 trigger → compaction → rewrite-deletes → expire 흐름까지 본다.

### 트러블슈팅

| 증상 | 원인 / 대응 |
|---|---|
| `docker exec` 로 시작한 task가 `permission denied` | `/var/run/docker.sock` 마운트가 안 됐거나 컨테이너가 root가 아닌 상태. compose의 `user: "0:0"` 설정 확인 |
| DAG이 Web UI에 안 뜸 | `dags/` 디렉토리 마운트 또는 파일 syntax 오류. `docker compose logs airflow-scheduler` 로 stack trace 확인 |
| `airflow-init` 가 계속 재시작됨 | postgres 가 아직 `pg_isready` 통과 전. `docker compose logs airflow-postgres` 로 확인 |
| `spark-iceberg: not found` | airflow와 spark-iceberg 가 같은 `lakehouse` 네트워크에 있는지 확인. compose 파일 수정 시 `--force-recreate` 권장 |

### 종료

```bash
docker compose --profile airflow down
# 메타데이터까지 초기화하려면
docker compose --profile airflow down -v
```

---

## 부록: S3 raw-only 파이프라인 실행 가이드

Iceberg (processed / summary) 계층을 만들지 않고 **Kafka -> S3 raw zone**까지만 운영하는 최소 구성. Glue / Athena도 사용하지 않는다.

### 필요한 파일만 정리

| 역할 | 파일 |
|---|---|
| Spark + Iceberg 이미지 | `Dockerfile` |
| 서비스 토폴로지 | `docker-compose.yml` (spark-iceberg + `--profile streaming`의 kafka / zookeeper / kafka-ui) |
| 실습 데이터 생성 | `generate_sample_data.py` 또는 `prepare_criteo_data.py` |
| Kafka producer | `kafka_producer.py` |
| Streaming ingest | `jobs/kafka_to_raw_files.py` |

`jobs/raw_to_processed_iceberg.py`, `jobs/processed_to_campaign_summary.py`, `jobs/local_stream_smoke.py`, `split_events.py`는 이 경로에서 필요 없다.

### 0단계: AWS 콘솔에서 실습 전용 IAM 사용자 생성 (Admin 권한)

회사 운영 계정과 섞이지 않게, 실습 전용 IAM 사용자를 하나 파고 거기에 Admin 권한을 붙인다. 실습 끝나면 비활성화하거나 삭제하면 된다.

1. **AWS 콘솔 로그인** -> 우상단 리전을 `Asia Pacific (Seoul) ap-northeast-2`로 설정.
2. 상단 검색창에서 **IAM** 진입.
3. 좌측 `Users` -> `Create user`.
   - User name: `iceberg-lab` (CLI 프로필 이름과 맞추면 편하다).
   - `Provide user access to the AWS Management Console`은 **끈다** (CLI만 쓸 거라).
4. `Set permissions` 화면 -> `Attach policies directly` -> **`AdministratorAccess`** 체크 -> Next -> Create user.
5. 생성된 사용자 상세 -> `Security credentials` 탭 -> `Create access key`.
   - Use case: `Command Line Interface (CLI)` 선택 -> 안내문 확인 -> `Create access key`.
   - 화면에 뜨는 `Access key ID`와 `Secret access key`를 **지금 바로** 복사한다. Secret은 이 화면을 벗어나면 다시 볼 수 없다.
6. 실습이 끝나면 해당 사용자의 Access key를 `Deactivate` -> `Delete`, 필요 없으면 사용자 자체를 삭제한다.

Admin 권한은 계정 전체에 대해 모든 작업이 가능하므로, 이 키가 유출되지 않도록 주의한다. 회사 운영 계정에서는 지양하고, 가능하면 개인 / 샌드박스 AWS 계정에서 발급하는 편이 낫다.

### 1단계: AWS CLI 설치 및 프로필 설정

macOS 기준.

```bash
brew install awscli
aws --version
```

실습 전용 프로필을 만든다. 기존 회사 계정과 섞이지 않게 이름을 따로 준다.

```bash
aws configure --profile iceberg-lab
# AWS Access Key ID     [None]: <access key>
# AWS Secret Access Key [None]: <secret key>
# Default region name   [None]: ap-northeast-2
# Default output format [None]: json
```

설정이 `~/.aws/credentials`와 `~/.aws/config`에 들어간다. 동작 확인.

```bash
aws sts get-caller-identity --profile iceberg-lab
aws s3 ls --profile iceberg-lab
```

필요 IAM 권한 (최소):
- 대상 버킷에 대한 `s3:PutObject`, `s3:GetObject`, `s3:ListBucket`
- Structured Streaming 체크포인트 갱신을 위한 `s3:DeleteObject`

### 2단계: S3 버킷 / prefix 준비

기존 버킷을 쓰거나 새로 만든다. 버킷 이름은 전 세계에서 유일해야 하므로, 아래 예시의 `metacode-iceberg-test`는 **본인이 만든 버킷 이름으로 반드시 교체**한다. 이후 4단계 `spark-submit` 명령의 `s3a://...` 경로에도 동일하게 반영해야 한다.

없으면 새로 생성.

```bash
# 본인 버킷 이름으로 교체
export BUCKET=<your-unique-bucket-name>
export PREFIX=lakehouse-lab

aws s3 mb s3://$BUCKET --profile iceberg-lab --region ap-northeast-2

# prefix placeholder 오브젝트 (선택, 콘솔에서 폴더처럼 보이게)
aws s3api put-object --bucket $BUCKET --key $PREFIX/ \
  --profile iceberg-lab --region ap-northeast-2
```

파이프라인이 사용하는 경로 (event-type 별로 3 zone):
- `s3://$BUCKET/$PREFIX/raw/impressions/`, `raw/clicks/`, `raw/conversions/` — 이벤트 타입별 parquet 적재
- `s3://$BUCKET/$PREFIX/checkpoints/raw-impressions/`, `raw-clicks/`, `raw-conversions/` — 각 streaming 잡의 체크포인트

### 3단계: 실습 데이터 생성과 Docker 기동

```bash
python3 generate_sample_data.py --events 100000

docker compose up -d --build              # spark-iceberg
docker compose --profile streaming up -d  # kafka + zookeeper + kafka-ui
```

기동 확인.

```bash
docker compose ps
```

### 4단계: 컨테이너에 AWS 자격증명 주입해서 streaming ingest

`docker-compose.yml`은 호스트의 `AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_DEFAULT_REGION`를 그대로 컨테이너로 넘긴다. 셸에서 먼저 export 한 뒤 `docker exec -e`로 한 번 더 전달한다.

producer가 3 토픽으로 분리 발행하므로, streaming 잡도 `--event-type` 별로 **세 개를 띄운다**.

```bash
export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id --profile iceberg-lab)
export AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key --profile iceberg-lab)
export AWS_DEFAULT_REGION=ap-northeast-2

# 공통 함수: event-type 별로 spark-submit 한 번씩 띄운다
run_stream() {
  local etype="$1"   # impression | click | conversion
  local zone="$2"    # impressions | clicks | conversions

  docker exec -d \
    -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_DEFAULT_REGION \
    spark-iceberg bash -c "/usr/local/spark/bin/spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
      /home/jovyan/jobs/kafka_to_raw_files.py \
      --kafka-packages '' \
      --bootstrap-servers kafka:29092 \
      --event-type ${etype} \
      --starting-offsets earliest \
      --raw-path s3a://$BUCKET/$PREFIX/raw/${zone} \
      --checkpoint-path s3a://$BUCKET/$PREFIX/checkpoints/raw-${zone} \
      > /tmp/kafka_stream_s3_${zone}.log 2>&1"
}

run_stream impression impressions
run_stream click       clicks
run_stream conversion  conversions
```

- `--topic` 은 생략했으므로 `--event-type` 에 따라 `ad-impressions` / `ad-clicks` / `ad-conversions` 가 자동 선택된다.
- `s3a://` 스킴을 처리하는 Hadoop S3A 커넥터와 AWS SDK는 `Dockerfile`의 `iceberg-aws-bundle`로 이미 classpath에 있다. 별도 `--packages` 없이 동작한다.
- Kafka 커넥터는 Spark 이미지에 없어서 `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3`는 필요하다.
- `docker exec -d`는 detach 모드이므로 명령이 바로 반환된다. 각 잡의 로그는 컨테이너 안 `/tmp/kafka_stream_s3_<zone>.log` 로 분리된다.

실행 상태 확인.

```bash
docker exec spark-iceberg bash -c "tail -n 5 /tmp/kafka_stream_s3_impressions.log /tmp/kafka_stream_s3_clicks.log /tmp/kafka_stream_s3_conversions.log"
# 각 잡에서 FileSink[s3a://.../raw/<zone>], KafkaV2[Subscribe[ad-<zone>]] 라인과
# "Streaming query has been idle..." 로그가 보이면 정상.
```

### 5단계: Kafka producer로 이벤트 발행

호스트에서 바로 쏘는 게 편하다 (`kafka-python-ng` 필요).

```bash
pip install kafka-python-ng==2.2.2

# 샘플 1,000건으로 빠르게
python3 kafka_producer.py \
  --csv ./data/ad_events_sample.csv \
  --speed 500 --max-events 1000

# 또는 전체
python3 kafka_producer.py --csv ./data/ad_events.csv --speed 500
```

### 6단계: S3 적재 검증

3 zone 모두 따로 확인한다.

```bash
for zone in impressions clicks conversions; do
  echo "== raw/$zone =="
  aws s3 ls s3://$BUCKET/$PREFIX/raw/$zone/ --recursive \
    --profile iceberg-lab --region ap-northeast-2 | wc -l
done

for zone in impressions clicks conversions; do
  echo "== checkpoints/raw-$zone =="
  aws s3 ls s3://$BUCKET/$PREFIX/checkpoints/raw-$zone/ \
    --profile iceberg-lab --region ap-northeast-2
done
```

- 파일이 zone 별로 `raw_date=YYYY-MM-DD/raw_hour=HH/` 아래 snappy parquet으로 쌓인다.
- impression이 가장 빨리, conversion이 가장 늦게 차오른다 (producer가 conversion을 지연 발행하기 때문).
- 체크포인트는 zone마다 별도 디렉토리로 `offsets/`, `commits/`, `sources/` 구조를 가진다.

### 7단계: Streaming 종료 / 재개

3개 잡이 같은 스크립트로 떠 있어, 한 번의 `pgrep` 으로 함께 죽일 수 있다.

```bash
docker exec spark-iceberg bash -c "pgrep -f kafka_to_raw_files.py | xargs -r kill"
```

체크포인트가 S3에 남아 있으므로 동일 `run_stream` 명령으로 다시 실행하면 각 zone이 마지막 Kafka offset부터 이어 읽는다. 처음부터 다시 읽고 싶으면 zone 별 체크포인트 디렉토리와 3 토픽을 모두 초기화한다.

```bash
for zone in impressions clicks conversions; do
  aws s3 rm s3://$BUCKET/$PREFIX/checkpoints/raw-$zone/ --recursive \
    --profile iceberg-lab --region ap-northeast-2
done

docker exec kafka bash -c '
for t in ad-impressions ad-clicks ad-conversions; do
  kafka-topics --bootstrap-server localhost:9092 --delete --topic "$t" || true
  kafka-topics --bootstrap-server localhost:9092 --create --topic "$t" --partitions 3 --replication-factor 1
done'
```

## 설계 이유

### 왜 raw를 Iceberg로 두지 않았는가

- raw의 주 목적은 보관, 재처리, 디버깅이다
- raw는 append-only면 충분하다
- raw까지 Iceberg로 두면 관리 포인트와 비용이 늘어난다
- MERGE / snapshot / compaction의 핵심 가치는 `processed_events` 이후에서 더 크다

### 왜 processed / summary는 Iceberg인가

- `processed_events`는 Conversion Delay 반영 때문에 MERGE가 필요하다
- `campaign_summary`는 incremental update와 snapshot 관리가 유리하다
- Athena / QuickSight / Spark에서 공통 테이블로 쓰기 좋다

## 강의와의 정렬

- 2회차: Medallion 구조와 3개 계층 분리는 그대로 유지
- 4회차: `Raw / Processing / Summary` 분리는 그대로 유지
- 달라지는 점: `raw_events`를 Iceberg Bronze로 두지 않고 raw zone files로 둔다

즉, 강의의 핵심 개념은 유지하면서도, 실습에서는 더 단순한 raw 계층을 선택한 구성이다.
