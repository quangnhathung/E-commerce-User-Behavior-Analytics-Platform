FROM eclipse-temurin:11-jre-jammy@sha256:1adc4db6079f9a35c45f36b0907dd94232a0b9b409a4088a7fe0c77b14e99294

RUN apt-get update && apt-get install -y --no-install-recommends bash ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY build/download.sh /usr/local/bin/download-verified
ARG HADOOP_VERSION=3.4.2
ARG HADOOP_SHA512=53582b3dae515316933c8486cdbd688a93530deb4bcc842e6a599f33dfeb51b17d79f61dc15b6febe95715325c2d2f4fc503bfae6ec3226851644ba7ebae8930
RUN --mount=type=cache,id=ecommerce-hadoop-distributions,target=/var/cache/apache,sharing=locked \
    bash /usr/local/bin/download-verified /var/cache/apache hadoop.tgz "${HADOOP_SHA512}" \
      "https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}-lean.tar.gz" \
      "https://repo.huaweicloud.com/apache/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}-lean.tar.gz" \
      "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}-lean.tar.gz" \
    && mkdir -p /opt/hadoop \
    && tar -xzf "/var/cache/apache/${HADOOP_SHA512}/hadoop.tgz" -C /opt/hadoop --strip-components=1
RUN groupadd --gid 1000 analytics && useradd --uid 1000 --gid analytics --create-home analytics \
    && mkdir -p /var/lib/hadoop/name /var/lib/hadoop/data /opt/hadoop/logs \
    && chown -R analytics:analytics /var/lib/hadoop /opt/hadoop/logs
ENV HADOOP_HOME=/opt/hadoop HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV PATH=/opt/hadoop/bin:/opt/hadoop/sbin:$PATH
COPY hadoop/entrypoint.sh /usr/local/bin/hadoop-entrypoint
USER analytics
ENTRYPOINT ["bash", "/usr/local/bin/hadoop-entrypoint"]
