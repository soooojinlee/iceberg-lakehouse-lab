-- ===========================================================================
-- Bronze (raw) zone external tables on Athena
--
-- Spark Structured Streaming이 S3에 쌓아 둔 plain parquet zone을 Athena에서
-- 읽기 위한 external table 정의.
--
-- Layout:
--   s3://metacode-study-datalake/ad_lakehouse/raw/impressions/raw_date=YYYY-MM-DD/raw_hour=H/*.parquet
--   s3://metacode-study-datalake/ad_lakehouse/raw/clicks/...
--   s3://metacode-study-datalake/ad_lakehouse/raw/conversions/...
--
-- Partition projection을 쓰므로 MSCK REPAIR / Glue Crawler 없이 바로 조회된다.
-- (storage.location.template으로 _spark_metadata/ 디렉터리는 자동 제외)
-- ===========================================================================

CREATE DATABASE IF NOT EXISTS ad_lakehouse
LOCATION 's3://metacode-study-datalake/ad_lakehouse/';


-- ---------------------------------------------------------------------------
-- raw_impressions
-- ---------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS ad_lakehouse.raw_impressions (
  kafka_partition INT,
  kafka_offset    BIGINT,
  kafka_timestamp TIMESTAMP,
  event_id        STRING,
  event_type      STRING,
  `timestamp`     BIGINT,
  event_time      STRING,
  uid             STRING,
  campaign        INT,
  cost            DOUBLE,
  event_timestamp TIMESTAMP,
  ingest_ts       TIMESTAMP
)
PARTITIONED BY (raw_date STRING, raw_hour INT)
STORED AS PARQUET
LOCATION 's3://metacode-study-datalake/ad_lakehouse/raw/impressions/'
TBLPROPERTIES (
  'projection.enabled'         = 'true',
  'projection.raw_date.type'   = 'date',
  'projection.raw_date.range'  = '2026-01-01,NOW',
  'projection.raw_date.format' = 'yyyy-MM-dd',
  'projection.raw_hour.type'   = 'integer',
  'projection.raw_hour.range'  = '0,23',
  'storage.location.template'  =
    's3://metacode-study-datalake/ad_lakehouse/raw/impressions/raw_date=${raw_date}/raw_hour=${raw_hour}/'
);


-- ---------------------------------------------------------------------------
-- raw_clicks  (cost 없음, impression_timestamp 추가)
-- ---------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS ad_lakehouse.raw_clicks (
  kafka_partition       INT,
  kafka_offset          BIGINT,
  kafka_timestamp       TIMESTAMP,
  event_id              STRING,
  event_type            STRING,
  `timestamp`           BIGINT,
  event_time            STRING,
  impression_timestamp  BIGINT,
  uid                   STRING,
  campaign              INT,
  event_timestamp       TIMESTAMP,
  ingest_ts             TIMESTAMP
)
PARTITIONED BY (raw_date STRING, raw_hour INT)
STORED AS PARQUET
LOCATION 's3://metacode-study-datalake/ad_lakehouse/raw/clicks/'
TBLPROPERTIES (
  'projection.enabled'         = 'true',
  'projection.raw_date.type'   = 'date',
  'projection.raw_date.range'  = '2026-01-01,NOW',
  'projection.raw_date.format' = 'yyyy-MM-dd',
  'projection.raw_hour.type'   = 'integer',
  'projection.raw_hour.range'  = '0,23',
  'storage.location.template'  =
    's3://metacode-study-datalake/ad_lakehouse/raw/clicks/raw_date=${raw_date}/raw_hour=${raw_hour}/'
);


-- ---------------------------------------------------------------------------
-- raw_conversions  (impression_timestamp + conversion_delay_sec 추가)
-- ---------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS ad_lakehouse.raw_conversions (
  kafka_partition        INT,
  kafka_offset           BIGINT,
  kafka_timestamp        TIMESTAMP,
  event_id               STRING,
  event_type             STRING,
  `timestamp`            BIGINT,
  event_time             STRING,
  impression_timestamp   BIGINT,
  uid                    STRING,
  campaign               INT,
  conversion_delay_sec   BIGINT,
  event_timestamp        TIMESTAMP,
  ingest_ts              TIMESTAMP
)
PARTITIONED BY (raw_date STRING, raw_hour INT)
STORED AS PARQUET
LOCATION 's3://metacode-study-datalake/ad_lakehouse/raw/conversions/'
TBLPROPERTIES (
  'projection.enabled'         = 'true',
  'projection.raw_date.type'   = 'date',
  'projection.raw_date.range'  = '2026-01-01,NOW',
  'projection.raw_date.format' = 'yyyy-MM-dd',
  'projection.raw_hour.type'   = 'integer',
  'projection.raw_hour.range'  = '0,23',
  'storage.location.template'  =
    's3://metacode-study-datalake/ad_lakehouse/raw/conversions/raw_date=${raw_date}/raw_hour=${raw_hour}/'
);


-- ---------------------------------------------------------------------------
-- Sanity check (실행 후 5000 / 211 / 4 가 나와야 정상)
-- ---------------------------------------------------------------------------
-- SELECT COUNT(*) AS impressions FROM ad_lakehouse.raw_impressions;
-- SELECT COUNT(*) AS clicks      FROM ad_lakehouse.raw_clicks;
-- SELECT COUNT(*) AS conversions FROM ad_lakehouse.raw_conversions;
