"""
Structured Streaming -> local file sink smoke test.

Kafka 없이 Spark 내장 rate source를 사용해 file sink가 동작하는지만 빠르게 확인한다.
출력 경로는 bind mount된 /home/jovyan/data 아래를 사용한다.
"""

import argparse
import shutil
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


def main():
    parser = argparse.ArgumentParser(description="Local file sink smoke test")
    parser.add_argument(
        "--output-path",
        default="/home/jovyan/data/raw-smoke",
        help="로컬 파일 sink 경로",
    )
    parser.add_argument(
        "--checkpoint-path",
        default="/home/jovyan/data/checkpoints/raw-smoke",
        help="checkpoint 경로",
    )
    parser.add_argument(
        "--duration-sec",
        type=int,
        default=6,
        help="스트림을 유지할 시간",
    )
    parser.add_argument(
        "--rows-per-second",
        type=int,
        default=5,
        help="rate source rowsPerSecond",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="실행 전 기존 output/checkpoint 삭제",
    )
    args = parser.parse_args()

    output_path = Path(args.output_path)
    checkpoint_path = Path(args.checkpoint_path)

    if args.clean:
        shutil.rmtree(output_path, ignore_errors=True)
        shutil.rmtree(checkpoint_path, ignore_errors=True)

    spark = SparkSession.builder.appName("LocalStreamSmoke").getOrCreate()

    df = (
        spark.readStream.format("rate")
        .option("rowsPerSecond", args.rows_per_second)
        .load()
        .withColumn("ingest_ts", current_timestamp())
    )

    query = (
        df.writeStream.format("parquet")
        .outputMode("append")
        .option("path", str(output_path))
        .option("checkpointLocation", str(checkpoint_path))
        .start()
    )

    time.sleep(args.duration_sec)
    query.stop()
    spark.stop()

    print(f"output_path={output_path}")
    print(f"checkpoint_path={checkpoint_path}")
    print("smoke-done")


if __name__ == "__main__":
    main()
