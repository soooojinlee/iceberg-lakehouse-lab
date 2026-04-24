# 광고 플랫폼 Lakehouse 실전 설계 - 실습 환경

이 repo는 강의 개요의 `Raw -> Processing -> Summary` 흐름은 유지하되, 저장 계층은 다음처럼 가져간다.

- `raw`: Iceberg 테이블이 아니라 **S3 / 로컬 파일 append zone**
- `processed_events`: **Iceberg**
- `campaign_summary`: **Iceberg**

즉, 기본 구조는 아래와 같다.

```text
Criteo CSV
  -> Kafka (ad-events)
  -> Spark Structured Streaming
  -> raw files on S3 or local
  -> Spark batch / incremental merge
  -> processed_events (Iceberg)
  -> campaign_summary (Iceberg)
```

이 구조는 강의 2회차의 Medallion 분리와 4회차의 `Raw / Processing / Summary` 구분에는 맞고, 기존 개요의 `raw_events` Iceberg Bronze만 파일 기반 raw zone으로 바꾼 실습 변형안이다.

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

기본 강의 경로는 단일 토픽이다.

```bash
# 기본: ad-events 단일 토픽
python kafka_producer.py --csv ./data/ad_events.csv

# 빠른 테스트
python kafka_producer.py --csv ./data/ad_events_sample.csv --speed 500

# 확장 실습: 3토픽 파생
python kafka_producer.py --realistic --csv ./data/ad_events.csv
```

## 4단계: Spark가 Kafka를 읽고 raw 파일로 적재

이 단계는 Kafka 메시지를 바로 Iceberg에 쓰지 않고, append-only raw zone에 파일로 쌓는다.

### 로컬 예시

```bash
docker compose exec spark-iceberg \
  /usr/local/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
    /home/jovyan/jobs/kafka_to_raw_files.py \
    --kafka-packages "" \
    --bootstrap-servers kafka:29092 \
    --topic ad-events \
    --raw-path /home/jovyan/warehouse/raw/ad-events \
    --checkpoint-path /home/jovyan/warehouse/checkpoints/raw-ad-events
```

### S3 예시

```bash
docker compose exec spark-iceberg \
  /usr/local/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
    /home/jovyan/jobs/kafka_to_raw_files.py \
    --kafka-packages "" \
    --bootstrap-servers kafka:29092 \
    --topic ad-events \
    --raw-path s3://<your-bucket>/raw/ad-events \
    --checkpoint-path s3://<your-bucket>/checkpoints/raw-ad-events
```

기본 출력 포맷은 `parquet`이며, raw 파일은 `raw_date`, `raw_hour` 기준으로 파티셔닝된다.
Kafka reader는 컨테이너 안에서 `python` 대신 `spark-submit`으로 실행해야 하며, 위 예시처럼 Kafka connector를 함께 붙이는 편이 안전하다.

## 5단계: raw files -> processed_events Iceberg

`processed_events`부터 Iceberg를 적용한다.

### 로컬 카탈로그 예시

```bash
docker compose exec spark-iceberg \
  /usr/local/spark/bin/spark-submit \
    /home/jovyan/jobs/raw_to_processed_iceberg.py \
    --catalog-mode local \
    --catalog-name local \
    --warehouse /home/jovyan/warehouse \
    --raw-path /home/jovyan/warehouse/raw/ad-events
```

### Glue Catalog + S3 예시

```bash
docker compose exec spark-iceberg \
  /usr/local/spark/bin/spark-submit \
    /home/jovyan/jobs/raw_to_processed_iceberg.py \
    --catalog-mode glue \
    --catalog-name glue_catalog \
    --warehouse s3://<your-bucket>/warehouse \
    --raw-path s3://<your-bucket>/raw/ad-events
```

기본 동작은 최근 7일 윈도우를 읽어 `processed_events`에 MERGE 한다.

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

파이프라인이 사용하는 경로:
- `s3://$BUCKET/$PREFIX/raw/ad-events/` — 이벤트 parquet 적재
- `s3://$BUCKET/$PREFIX/checkpoints/raw-ad-events/` — Structured Streaming 체크포인트

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

```bash
export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id --profile iceberg-lab)
export AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key --profile iceberg-lab)
export AWS_DEFAULT_REGION=ap-northeast-2

docker exec -d \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_DEFAULT_REGION \
  spark-iceberg bash -c "/usr/local/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
    /home/jovyan/jobs/kafka_to_raw_files.py \
    --kafka-packages '' \
    --bootstrap-servers kafka:29092 \
    --topic ad-events \
    --starting-offsets earliest \
    --raw-path s3a://$BUCKET/$PREFIX/raw/ad-events \
    --checkpoint-path s3a://$BUCKET/$PREFIX/checkpoints/raw-ad-events \
    > /tmp/kafka_stream_s3.log 2>&1"
```

- `s3a://` 스킴을 처리하는 Hadoop S3A 커넥터와 AWS SDK는 `Dockerfile`의 `iceberg-aws-bundle`로 이미 classpath에 있다. 별도 `--packages` 없이 동작한다.
- Kafka 커넥터는 Spark 이미지에 없어서 `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3`는 필요하다.
- `docker exec -d`는 detach 모드이므로 명령이 바로 반환된다. 로그는 컨테이너 안 `/tmp/kafka_stream_s3.log`.

실행 상태 확인.

```bash
docker exec spark-iceberg tail -f /tmp/kafka_stream_s3.log
# 정상이면 FileSink[s3a://.../raw/ad-events], KafkaV2[Subscribe[ad-events]] 라인과
# "Streaming query has been idle..." 로그가 보인다.
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

```bash
aws s3 ls s3://$BUCKET/$PREFIX/raw/ad-events/ --recursive \
  --profile iceberg-lab --region ap-northeast-2 | wc -l

aws s3 ls s3://$BUCKET/$PREFIX/checkpoints/raw-ad-events/ \
  --profile iceberg-lab --region ap-northeast-2
```

- 파일이 `raw_date=YYYY-MM-DD/raw_hour=HH/` 아래 snappy parquet로 쌓인다.
- 체크포인트는 `offsets/`, `commits/`, `sources/` 디렉토리로 구성된다.

### 7단계: Streaming 종료 / 재개

```bash
docker exec spark-iceberg bash -c "pgrep -f kafka_to_raw_files.py | xargs -r kill"
```

체크포인트가 S3에 남아 있으므로 동일 명령으로 다시 실행하면 마지막 Kafka offset부터 이어 읽는다. 처음부터 다시 읽고 싶으면 체크포인트 디렉토리를 지우고 토픽도 재생성한다.

```bash
aws s3 rm s3://$BUCKET/$PREFIX/checkpoints/raw-ad-events/ --recursive \
  --profile iceberg-lab --region ap-northeast-2

docker exec kafka bash -c "kafka-topics --bootstrap-server localhost:9092 --delete --topic ad-events; \
  kafka-topics --bootstrap-server localhost:9092 --create --topic ad-events --partitions 3 --replication-factor 1"
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
