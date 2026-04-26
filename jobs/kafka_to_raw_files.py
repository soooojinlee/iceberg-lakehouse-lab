"""
Spark Structured Streaming: Kafka -> raw files (event-type 별 분리 zone).

raw zone는 Iceberg 테이블이 아니라 append-only 파일 zone이다. 한 spark-submit이
한 토픽 -> 한 raw zone 을 처리하며, --event-type 으로 schema와 default topic을 결정한다.

  --event-type impression  ->  ad-impressions  ->  raw/impressions/
  --event-type click       ->  ad-clicks       ->  raw/clicks/
  --event-type conversion  ->  ad-conversions  ->  raw/conversions/

각 zone은 자체 schema의 plain parquet로 적재되고 (raw_date / raw_hour 파티션),
silver layer (raw_to_processed_iceberg.py) 가 event_id 기준으로 조립한다.
"""

import argparse


EVENT_TYPE_DEFAULT_TOPIC = {
    "impression": "ad-impressions",
    "click": "ad-clicks",
    "conversion": "ad-conversions",
}


def build_event_schema(event_type):
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    common = [
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("timestamp", LongType()),
        StructField("event_time", StringType()),
        StructField("uid", StringType()),
        StructField("campaign", IntegerType()),
    ]

    if event_type == "impression":
        return StructType(common + [StructField("cost", DoubleType())])
    if event_type == "click":
        return StructType(
            [common[0], common[1], common[2], common[3]]
            + [StructField("impression_timestamp", LongType())]
            + [common[4], common[5]]
        )
    if event_type == "conversion":
        return StructType(
            [common[0], common[1], common[2], common[3]]
            + [StructField("impression_timestamp", LongType())]
            + [common[4], common[5]]
            + [StructField("conversion_delay_sec", LongType())]
        )
    raise ValueError(f"unsupported event-type: {event_type}")


def build_spark(app_name, kafka_packages):
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
    )
    if kafka_packages:
        builder = builder.config("spark.jars.packages", kafka_packages)
    return builder.getOrCreate()


def main():
    parser = argparse.ArgumentParser(description="Kafka -> raw files streaming job (event-type 분리)")
    parser.add_argument(
        "--event-type",
        choices=list(EVENT_TYPE_DEFAULT_TOPIC.keys()),
        required=True,
        help="처리할 이벤트 타입. schema와 default topic을 결정한다.",
    )
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument(
        "--topic",
        default=None,
        help="Kafka 토픽 (생략 시 event-type 기본값: impression->ad-impressions 등)",
    )
    parser.add_argument("--starting-offsets", default="earliest")
    parser.add_argument(
        "--raw-path",
        required=True,
        help="event-type 별 zone 경로. 예: /home/jovyan/warehouse/raw/impressions",
    )
    parser.add_argument("--checkpoint-path", required=True)
    parser.add_argument("--output-format", choices=["parquet", "json"], default="parquet")
    parser.add_argument(
        "--kafka-packages",
        default="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        help="Spark Kafka connector coordinates. 빈 문자열이면 추가 설정 안 함",
    )
    args = parser.parse_args()

    topic = args.topic or EVENT_TYPE_DEFAULT_TOPIC[args.event_type]

    from pyspark.sql.functions import (
        col,
        current_timestamp,
        from_json,
        from_unixtime,
        hour,
        to_date,
        to_timestamp,
    )

    spark = build_spark(f"KafkaToRawFiles[{args.event_type}]", args.kafka_packages)
    schema = build_event_schema(args.event_type)

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", args.starting_offsets)
        .load()
    )

    parsed = (
        kafka_df.select(
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), schema).alias("data"),
        )
        .select("kafka_partition", "kafka_offset", "kafka_timestamp", "data.*")
        .withColumn("event_timestamp", to_timestamp(from_unixtime(col("timestamp"))))
        .withColumn("ingest_ts", current_timestamp())
        .withColumn("raw_date", to_date(col("ingest_ts")))
        .withColumn("raw_hour", hour(col("ingest_ts")))
    )

    query = (
        parsed.writeStream.format(args.output_format)
        .outputMode("append")
        .option("checkpointLocation", args.checkpoint_path)
        .option("path", args.raw_path)
        .partitionBy("raw_date", "raw_hour")
        .start()
    )

    print(f"event-type:  {args.event_type}")
    print(f"topic:       {topic}")
    print(f"raw path:    {args.raw_path}")
    print(f"checkpoint:  {args.checkpoint_path}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
