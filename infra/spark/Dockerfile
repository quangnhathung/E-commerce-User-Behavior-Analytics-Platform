FROM python:3.11-slim-bookworm@sha256:0a310eeecf4e1f5a0743f9a6520c90c88d089c903ca5fd283f501e3a805f5f89

ARG SPARK_VERSION=3.5.7
ARG SPARK_SHA512=f3b7d5974d746b9aaecb19104473da91068b698a4d292177deb75deb83ef9dc7eb77062446940561ac9ab7ee3336fb421332b1c877292dab4ac1b6ca30f4f2e0
RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless bash ca-certificates curl procps \
    && rm -rf /var/lib/apt/lists/* \
    && curl --fail --location --retry 4 --connect-timeout 30 \
       "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" -o /tmp/spark.tgz \
    && echo "${SPARK_SHA512}  /tmp/spark.tgz" | sha512sum --check --status \
    && mkdir -p /opt/spark \
    && tar -xzf /tmp/spark.tgz -C /opt/spark --strip-components=1 \
    && rm /tmp/spark.tgz \
    && groupadd --gid 1000 analytics && useradd --uid 1000 --gid analytics --create-home analytics \
    && mkdir -p /tmp/spark-local /tmp/spark-work /etc/hadoop \
    && chown -R analytics:analytics /tmp/spark-local /tmp/spark-work
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 SPARK_HOME=/opt/spark
ENV PATH=/opt/spark/bin:/opt/spark/sbin:$PATH \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    PYSPARK_PYTHON=/usr/local/bin/python3 PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3 \
    PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY requirements.txt download_jars.py /opt/build/
RUN pip install --no-cache-dir -r /opt/build/requirements.txt \
    && python3 /opt/build/download_jars.py \
    && rm -rf /opt/build \
    && python3 -c 'import pyspark, pymongo, confluent_kafka; assert pyspark.__version__ == "3.5.7"'
USER analytics
WORKDIR /home/analytics
CMD ["sleep", "infinity"]
