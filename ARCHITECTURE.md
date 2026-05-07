# 7회차 — 자동화 + BI 실습 환경 구성

이 문서는 `7th-orchestration` 브랜치의 *전체 실행 흐름* 과 *환경 구성* 을 한 장에 정리한다.
1~6회차에서 만든 조각들이 Airflow + Glue + S3 위에서 자동으로 도는 *광고 데이터 플랫폼* 이다.

---

## 1. 컨테이너 구성 (docker-compose)

| 서비스 | 이미지 | 포트 (host:container) | profile | 역할 |
|---|---|---|---|---|
| `spark-iceberg` | 자체 빌드 (`Dockerfile`) | 8888, 4040 | (default) | Spark 3.5.3 + Iceberg 1.5.2 + AWS bundle + Kafka jars + Jupyter |
| `kafka` | `confluentinc/cp-kafka:7.6.0` | 9092 | streaming | 이벤트 토픽 3개 (`ad-impressions`, `ad-clicks`, `ad-conversions`) |
| `zookeeper` | `confluentinc/cp-zookeeper:7.6.0` | — | streaming | Kafka 메타데이터 |
| `kafka-ui` | `provectuslabs/kafka-ui` | 8090:8080 | streaming | 토픽 시각화 |
| `airflow-postgres` | `postgres:13` | — | airflow | Airflow 메타데이터 DB |
| `airflow-init` | 자체 빌드 (`Dockerfile.airflow`) | — | airflow | DB migrate + admin 사용자 생성 (one-shot) |
| `airflow-webserver` | 자체 빌드 (`Dockerfile.airflow`) | 8080 | airflow | Airflow Web UI (admin / admin) |
| `airflow-scheduler` | 자체 빌드 (`Dockerfile.airflow`) | — | airflow | DAG 스케줄링 + 실행 |

### Airflow ↔ Spark 통신 패턴

Airflow는 `BashOperator`로 `docker exec spark-iceberg /usr/local/spark/bin/spark-submit ...` 를 호출한다.
이를 위해 Airflow 컨테이너에는:
- `Dockerfile.airflow`가 `docker.io` (Docker CLI) 를 추가 설치
- `/var/run/docker.sock` 마운트 (read-write)
- `user: "0:0"` 로 root 실행 (lab 단순화)

### Spark 이미지에 박힌 jars

`Dockerfile`이 빌드 시 `/usr/local/spark/jars/`에 다음을 wget:

| jar | 용도 |
|---|---|
| `iceberg-spark-runtime-3.5_2.12-1.5.2.jar` | Iceberg SQL extensions, catalog |
| `iceberg-aws-bundle-1.5.2.jar` | Glue catalog + S3FileIO |
| `hadoop-aws-3.3.4.jar` + `aws-java-sdk-bundle-1.12.262.jar` | `s3a://` 스킴 |
| `spark-sql-kafka-0-10_2.12-3.5.3.jar` + `spark-token-provider-kafka-0-10_2.12-3.5.3.jar` + `kafka-clients-3.4.1.jar` + `commons-pool2-2.11.1.jar` | Structured Streaming Kafka source |

이렇게 박아두는 이유: 컨테이너 재시작 시 `~/.ivy2` 휘발 + `--packages` Ivy resolution 실패 가능성을 사전 차단. *재현 가능한 lab*.

---

## 2. 환경 변수 (`.env`)

`docker compose`가 자동으로 읽는 파일.

```bash
ICEBERG_CATALOG_NAME=glue_catalog
ICEBERG_WAREHOUSE=s3://metacode-iceberg-test/lakehouse-lab/warehouse
ICEBERG_DATABASE=ad_lakehouse
ICEBERG_RAW_BASE=s3a://metacode-iceberg-test/lakehouse-lab/raw
```

### 스킴 차이의 의미

- **`s3://`** (warehouse) — Iceberg `S3FileIO`가 처리. AWS SDK v2 직접 사용. Glue 카탈로그가 가리키는 metadata.json도 이 스킴.
- **`s3a://`** (raw zone) — Hadoop FileSystem이 처리. `spark.read.format("parquet").load(...)`가 사용하는 일반 경로.

같은 S3 객체에 두 라이브러리가 다른 스킴으로 접근하는 정상 패턴.

### AWS 자격증명

`spark-iceberg` 컨테이너의 `volumes:`에 `${HOME}/.aws:/home/jovyan/.aws:ro` 마운트.
`AWS_PROFILE=iceberg-lab` env로 `~/.aws/credentials`에서 키를 읽음.
Airflow → docker exec로 spark-iceberg 호출 시 spark가 자기 마운트된 자격으로 인증.

---

## 3. 데이터 흐름 (End-to-End)

```
generate_sample_data.py             ── CSV 생성기
   │ 동적 시작일: 마지막 날 = 오늘 (silver 7일 윈도우와 매칭)
   ▼
data/ad_events.csv (전체)
data/ad_events_sample.csv (1,000건)
data/ad_events_batch2.csv (2nd append용)
   │
   ▼ kafka_producer.py --csv ... --speed 500 --delay-scale 0.0001
   │ CSV 한 행 → impression 즉시 / click 후 1~30초 / conversion 후 (delay × scale)
   │
   ▼
Kafka 3 토픽
   ad-impressions   ad-clicks   ad-conversions
   │
   ▼ jobs/kafka_to_raw_files.py (3개 spark-submit, --event-type 별)
   │ from_json 으로 schema 적용 + raw_date / raw_hour 파티션 컬럼 추가
   │
   ▼
S3 raw zone (Bronze, plain parquet)
   s3a://.../lakehouse-lab/raw/{impressions,clicks,conversions}/
     raw_date=YYYY-MM-DD/raw_hour=HH/*.parquet
   │
   ▼ Airflow DAG: ad_lakehouse_silver  (매시간 :00 UTC)
   │   check_raw → insert → update → compact → rewrite-deletes → expire → orphan(Sun)
   │   (jobs/raw_to_processed_iceberg.py + jobs/iceberg_maintenance.py)
   │
   ▼
Glue + S3 Iceberg (Silver, MOR)
   glue_catalog.ad_lakehouse.processed_events
   s3://.../warehouse/ad_lakehouse.db/processed_events/
   │
   ▼ Airflow DAG: ad_lakehouse_gold  (매시간 :30 UTC)
   │   insert → update → compact → rewrite-deletes → expire → orphan(Sun)
   │   (jobs/processed_to_campaign_summary.py + jobs/iceberg_maintenance.py)
   │
   ▼
Glue + S3 Iceberg (Gold, MOR)
   glue_catalog.ad_lakehouse.campaign_summary
   s3://.../warehouse/ad_lakehouse.db/campaign_summary/
   │
   ▼ Athena (Iceberg native, Engine v3+)
   │
   ▼ QuickSight (SPICE 또는 Direct query)
   비즈니스 KPI 대시보드 (CTR, CVR, CPA, 일자별 트렌드)
```

---

## 4. 레이어별 데이터 형태

### Bronze (raw, plain parquet, append-only)

3 zone 으로 *분리* — Iceberg가 아니다.

| Zone | 위치 | event-type 별 핵심 컬럼 |
|---|---|---|
| `impressions` | `s3a://.../raw/impressions/raw_date=YYYY-MM-DD/raw_hour=HH/*.parquet` | event_id, event_type, timestamp, event_time, uid, campaign, **cost** |
| `clicks` | `s3a://.../raw/clicks/...` | event_id, event_type, timestamp, **impression_timestamp**, event_time, uid, campaign |
| `conversions` | `s3a://.../raw/conversions/...` | event_id, event_type, timestamp, **impression_timestamp**, event_time, uid, campaign, **conversion_delay_sec** |

세 zone 공통 추가 컬럼:
- Kafka 메타: `kafka_partition`, `kafka_offset`, `kafka_timestamp`
- Streaming 추가: `event_timestamp` (epoch→TS 변환), `ingest_ts` (current_timestamp), `raw_date`, `raw_hour` (파티션 키)

**왜 raw는 Iceberg가 아닌가**: append-only / 보관·재처리·디버깅용. snapshot·MERGE의 가치는 silver 이후에 더 크다.

### Silver `glue_catalog.ad_lakehouse.processed_events` (Iceberg, MOR)

```sql
event_id              STRING       -- PK 역할
event_date            DATE         -- 파티션 키 (impression의 event_timestamp에서 파생)
uid                   STRING
campaign              INT
click                 INT          -- 0 또는 1
conversion            INT          -- 0 또는 1
conversion_delay_sec  BIGINT       -- impression → conversion 지연
cost                  DOUBLE
updated_at            TIMESTAMP

PARTITIONED BY (event_date)
TBLPROPERTIES (
  'format-version'      = '2',
  'write.update.mode'   = 'merge-on-read',
  'write.merge.mode'    = 'merge-on-read',
  'write.delete.mode'   = 'merge-on-read'
)
```

**조립 방식**: Bronze 3 zone을 `event_id` 기준 LEFT JOIN.
- `imp` (base) + `clk` (left) + `cnv` (left)
- click_flag = 1 if 도착, 0 if not (`coalesce(click_flag, 0)`)
- conversion_flag = 1 if 도착, 0 if not
- 늦게 도착한 conversion → 다음 silver UPDATE에서 `WHEN MATCHED` 분기 발화

### Gold `glue_catalog.ad_lakehouse.campaign_summary` (Iceberg, MOR)

```sql
summary_date    DATE      -- 파티션 키
campaign        INT       -- 복합 키
impressions     BIGINT    -- COUNT(*)
clicks          BIGINT    -- SUM(click)
conversions     BIGINT    -- SUM(conversion)
total_cost      DOUBLE    -- SUM(cost)
ctr             DOUBLE    -- clicks * 100 / impressions
cvr             DOUBLE    -- conversions * 100 / clicks
cpa             DOUBLE    -- total_cost / conversions
updated_at      TIMESTAMP

PARTITIONED BY (summary_date)
TBLPROPERTIES ( ... 'merge-on-read' ... )
```

**집계 방식**: `processed_events`를 `(event_date, campaign)`로 GROUP BY → `summary` 에 INSERT/UPDATE.
7일 윈도우 재집계 — silver의 conversion delay update가 KPI까지 자동 전파.

---

## 5. DAG 구조

### `ad_lakehouse_silver` — 매시간 :00 UTC

```
check_raw_zones_have_data
  └─ insert_new_events            (mode=insert, anti-join)
        └─ update_late_arrivals    (mode=update, MERGE WHEN MATCHED only)
              └─ compact_processed_events
                    └─ rewrite_position_deletes
                          └─ expire_snapshots
                                └─ remove_orphan_files_weekly  (UTC 일요일에만)
```

### `ad_lakehouse_gold` — 매시간 :30 UTC (silver 후 30분 오프셋)

```
insert_new_summary               (mode=insert, anti-join, KPI 한 번에 계산)
  └─ update_existing_summary      (mode=update, KPI 재계산)
        └─ compact_campaign_summary
              └─ rewrite_position_deletes
                    └─ expire_snapshots
                          └─ remove_orphan_files_weekly
```

### INSERT/UPDATE 분리의 의미

5회차에서 다룬 MERGE INTO의 두 분기 (`WHEN NOT MATCHED THEN INSERT` + `WHEN MATCHED THEN UPDATE`)를 *별도 task* 로 쪼갰다.

- **INSERT (`--mode insert`)**: `LEFT ANTI JOIN target`으로 새 키만 추림 → cheap, write-heavy 정상 흐름.
- **UPDATE (`--mode update`)**: 기존 키만 골라 conversion/click 늦게 도착한 경우 갱신 → 드물지만 무거움.

시연 자리에서 Graph view로 두 분기를 *눈으로 볼 수 있게* 분리.

### 매 사이클 maintenance 까지 포함하는 이유

- 작은 lab 환경에서 *한 사이클이 곧 완성된 라이프사이클* (insert → merge → compact → expire) — 시연 흐름 깔끔.
- 운영 환경에서는 보통 maintenance 주기를 분리 (compaction은 일/주, expire는 일).
- MOR 테이블은 매 MERGE 마다 position delete 파일 누적 → compaction + rewrite-deletes가 곧바로 따라가야 read 비용 안 뜸.

---

## 6. S3 경로 매트릭스

| 단계 | 작업 | 경로 |
|---|---|---|
| streaming → raw | 쓰기 | `s3a://metacode-iceberg-test/lakehouse-lab/raw/{impressions,clicks,conversions}/raw_date=YYYY-MM-DD/raw_hour=HH/*.parquet` |
| streaming checkpoint | 쓰기 | `s3a://.../lakehouse-lab/checkpoints/raw-{zone}/` |
| silver INSERT/UPDATE | raw 읽기 | 위 raw 경로와 동일 |
| silver INSERT/UPDATE | Iceberg 쓰기 | `s3://.../lakehouse-lab/warehouse/ad_lakehouse.db/processed_events/` (Glue 등록) |
| gold INSERT/UPDATE | Iceberg 읽기 | `glue_catalog.ad_lakehouse.processed_events` (Glue 메타 lookup) |
| gold INSERT/UPDATE | Iceberg 쓰기 | `s3://.../warehouse/ad_lakehouse.db/campaign_summary/` |

**bucket / prefix 동일** — `metacode-iceberg-test` / `lakehouse-lab`.

---

## 7. 실행 순서 (시연용)

### 0) 사전 준비

```bash
# 호스트 venv 셋업 (CSV 생성, kafka_producer 호출용)
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# AWS 자격증명 (~/.aws/credentials 의 iceberg-lab 프로필)
aws configure --profile iceberg-lab
```

### 1) 컨테이너 기동

```bash
# spark-iceberg
docker compose up -d --build

# kafka + zookeeper + kafka-ui
docker compose --profile streaming up -d

# Airflow (postgres + init + webserver + scheduler)
docker compose --profile airflow up -d --build
```

확인:
- Airflow Web UI: `http://localhost:8080`  (admin / admin)
- Spark UI: `http://localhost:4040` (job 실행 중에만)
- Kafka UI: `http://localhost:8090`

### 2) 한 사이클 자동 실행

```bash
./scripts/run_demo_cycle.sh
```

스크립트가 하는 일:
1. 컨테이너 상태 / AWS 자격 검증
2. 샘플 데이터 생성 (없으면) — 동적 시작일 (오늘 - 6일 ~ 오늘)
3. Kafka 토픽 생성 + producer 발행 (백그라운드)
4. 3개 streaming spark-submit (S3 raw zone에 적재)
5. 45초 대기 → 객체 카운트
6. `ad_lakehouse_silver` manual trigger + 완료 대기
7. `ad_lakehouse_gold` manual trigger + 완료 대기
8. spark-sql 로 row count + snapshot 요약 출력

### 3) 결과 확인

**Airflow UI** (`http://localhost:8080`):
- DAGs 탭에서 silver / gold 두 DAG의 Graph view
- 각 task 의 Log 클릭 → spark-submit stdout 그대로 보임

**AWS 콘솔**:
- Glue → Databases → `ad_lakehouse` → `processed_events`, `campaign_summary` 테이블
- S3 → `metacode-iceberg-test/lakehouse-lab/warehouse/ad_lakehouse.db/...` 데이터 + 메타파일

**Athena 쿼리**:
```sql
SELECT * FROM ad_lakehouse.campaign_summary
ORDER BY summary_date DESC, campaign;

-- Iceberg 메타테이블
SELECT * FROM ad_lakehouse."processed_events$snapshots" ORDER BY committed_at DESC;
SELECT * FROM ad_lakehouse."processed_events$files";
```

**QuickSight**:
- New dataset → Athena → `ad_lakehouse` / `campaign_summary`
- KPI cards / 일자별 트렌드 / 캠페인별 성과 / 전환 퍼널

---

## 8. 정리 / 종료

```bash
# Airflow 만 끄기
docker compose --profile airflow down

# 전체 끄기
docker compose --profile airflow --profile streaming down

# 메타데이터까지 초기화
docker compose --profile airflow --profile streaming down -v
```

S3 정리는 별도:

```bash
aws s3 rm s3://metacode-iceberg-test/lakehouse-lab/ --recursive --profile iceberg-lab
aws glue delete-database --name ad_lakehouse --profile iceberg-lab
```

---

## 9. 디렉토리 구조 (7회차 추가/변경분)

```
iceberg-lakehouse-lab/
├── .env                              ← 신규: ICEBERG_* 기본값
├── ARCHITECTURE.md                   ← 신규: 이 문서
├── Dockerfile                        ← 수정: kafka jars 4개 추가
├── Dockerfile.airflow                ← 신규: airflow + docker CLI
├── docker-compose.yml                ← 수정: airflow profile (postgres+init+webserver+scheduler)
├── README.md                         ← 수정: 7단계 + MOR + 부트스트랩
├── generate_sample_data.py           ← 수정: --start-date 동적 기본값
├── dags/
│   ├── ad_lakehouse_silver.py        ← 신규: silver 라이프사이클 (insert→update→maint)
│   └── ad_lakehouse_gold.py          ← 신규: gold 라이프사이클 (insert→update→maint)
├── jobs/
│   ├── kafka_to_raw_files.py         ← (4회차) 그대로
│   ├── raw_to_processed_iceberg.py   ← 수정: insert/update 모드 분리, MOR DDL, empty zone fallback
│   ├── processed_to_campaign_summary.py ← 수정: insert/update 모드 분리, MOR DDL
│   └── iceberg_maintenance.py        ← 신규: compact / rewrite-deletes / expire / orphan
└── scripts/
    └── run_demo_cycle.sh             ← 신규: 한 사이클 부트스트랩
```
