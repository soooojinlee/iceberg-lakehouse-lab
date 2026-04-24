######################################################################
# Jupyter + PySpark + Apache Iceberg 실습 이미지
#
# Base: jupyter/pyspark-notebook (Spark 3.5.x)
# 추가: Iceberg Runtime JAR, AWS Bundle, Python 패키지
######################################################################

FROM quay.io/jupyter/pyspark-notebook:spark-3.5.3

USER root

# Iceberg / S3A JARs를 Spark classpath에 추가
# - iceberg-spark-runtime: Iceberg SQL 확장, 카탈로그, 읽기/쓰기
# - iceberg-aws-bundle: S3 FileIO, Glue Catalog 연동 (SDK v2)
# - hadoop-aws + aws-java-sdk-bundle: raw zone 등 s3a:// 경로 읽기/쓰기 (SDK v1)
ENV ICEBERG_VERSION=1.5.2
ENV SPARK_MAJOR=3.5
ENV SCALA_MAJOR=2.12
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_BUNDLE_VERSION=1.12.262

RUN cd /usr/local/spark/jars && \
    wget -q "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR}_${SCALA_MAJOR}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR}_${SCALA_MAJOR}-${ICEBERG_VERSION}.jar" && \
    wget -q "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" && \
    wget -q "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" && \
    wget -q "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"

# s3a 가 AWS_PROFILE + ~/.aws/credentials 를 쓰도록 기본 자격 공급자 체인 지정
RUN echo "spark.hadoop.fs.s3a.aws.credentials.provider com.amazonaws.auth.DefaultAWSCredentialsProviderChain" \
      >> /usr/local/spark/conf/spark-defaults.conf

# Python 패키지
# boto3: S3/Glue/IAM 확인 스크립트 (가이드 문서에서 사용)
RUN pip install --no-cache-dir \
    kafka-python-ng==2.2.2 \
    faker==28.0.0 \
    pandas \
    pyarrow \
    boto3

# Warehouse 디렉토리 생성
RUN mkdir -p /home/jovyan/warehouse && \
    chown -R ${NB_UID}:${NB_GID} /home/jovyan/warehouse

USER ${NB_UID}

WORKDIR /home/jovyan/work
