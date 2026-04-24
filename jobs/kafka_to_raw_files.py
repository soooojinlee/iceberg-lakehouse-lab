"""
Spark Structured Streaming: Kafka -> raw files.

raw는 Iceberg 테이블이 아니라 append-only 파일 zone으로 둔다.
기본 포맷은 parquet이며 raw_date / raw_hour 단위로 저장한다.
"""

import argparse


def build_event_schema():
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("event_id", StringType()),
            StructField("event_type", StringType()),
            StructField("timestamp", LongType()),
            StructField("event_time", StringType()),
            StructField("uid", StringType()),
            StructField("campaign", IntegerType()),
            StructField("click", IntegerType()),
            StructField("conversion", IntegerType()),
            StructField("conversion_timestamp", LongType()),
            StructField("cost", DoubleType()),
        ]
    )


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
    parser = argparse.ArgumentParser(description="Kafka -> raw files streaming job")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="ad-events")
    parser.add_argument("--starting-offsets", default="earliest")
    parser.add_argument("--raw-path", required=True)
    parser.add_argument("--checkpoint-path", required=True)
    parser.add_argument("--output-format", choices=["parquet", "json"], default="parquet")
    parser.add_argument(
        "--kafka-packages",
        default="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        help="Spark Kafka connector coordinates. 빈 문자열이면 추가 설정 안 함",
    )
    args = parser.parse_args()

    from pyspark.sql.functions import (
        col,
        current_timestamp,
        from_json,
        hour,
        to_date,
        to_timestamp,
        from_unixtime,
    )

    spark = build_spark("KafkaToRawFiles", args.kafka_packages)
    schema = build_event_schema()

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
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

    print(f"raw path: {args.raw_path}")
    print(f"checkpoint: {args.checkpoint_path}")
    print(f"topic: {args.topic}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
