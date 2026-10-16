FROM python:3.11-slim-bookworm@sha256:0a310eeecf4e1f5a0743f9a6520c90c88d089c903ca5fd283f501e3a805f5f89

RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless bash ca-certificates curl procps \
    && rm -rf /var/lib/apt/lists/*
COPY build/download.sh /usr/local/bin/download-verified
ARG SPARK_VERSION=3.5.7
ARG SPARK_SHA512=f3b7d5974d746b9aaecb19104473da91068b698a4d292177deb75deb83ef9dc7eb77062446940561ac9ab7ee3336fb421332b1c877292dab4ac1b6ca30f4f2e0
RUN --mount=type=cache,id=ecommerce-spark-distributions,target=/var/cache/apache,sharing=locked \
    bash /usr/local/bin/download-verified /var/cache/apache spark.tgz "${SPARK_SHA512}" \
      "https://repo.huaweicloud.com/apache/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
      "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
    && mkdir -p /opt/spark \
    && tar -xzf "/var/cache/apache/${SPARK_SHA512}/spark.tgz" -C /opt/spark --strip-components=1
RUN groupadd --gid 1000 analytics && useradd --uid 1000 --gid analytics --create-home analytics \
    && mkdir -p /tmp/spark-local /tmp/spark-work /etc/hadoop \
    && chown -R analytics:analytics /tmp/spark-local /tmp/spark-work
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 SPARK_HOME=/opt/spark
ENV PATH=/opt/spark/bin:/opt/spark/sbin:$PATH \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    PYSPARK_PYTHON=/usr/local/bin/python3 PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3 \
    PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY spark/requirements.txt /opt/build/requirements.txt
RUN --mount=type=cache,id=ecommerce-spark-pip,target=/root/.cache/pip,sharing=locked \
    pip install -r /opt/build/requirements.txt
COPY spark/download_jars.py /opt/build/download_jars.py
RUN python3 /opt/build/download_jars.py \
    && rm -rf /opt/build \
    && python3 -c 'import pyspark, pymongo, confluent_kafka; assert pyspark.__version__ == "3.5.7"'
USER analytics
WORKDIR /home/analytics
CMD ["sleep", "infinity"]
