ARG CLICKHOUSE_VERSION=latest
FROM clickhouse/clickhouse-keeper:${CLICKHOUSE_VERSION}-alpine AS keeper

FROM python:3.10-bookworm

ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8

RUN apt-get update -qq && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y locales netcat-openbsd supervisor && \
    echo "en_US.UTF-8 UTF-8" > /etc/locale.gen && \
    locale-gen && \
    rm -rf /var/lib/apt/lists/*

RUN groupadd --system clickhouse && \
    useradd --system --gid clickhouse clickhouse && \
    mkdir -p /etc/clickhouse-keeper \
             /var/lib/clickhouse-keeper/coordination/log \
             /var/lib/clickhouse-keeper/coordination/snapshots \
             /var/log/clickhouse-keeper && \
    chown -R clickhouse:clickhouse \
        /etc/clickhouse-keeper \
        /var/lib/clickhouse-keeper \
        /var/log/clickhouse-keeper

COPY --from=keeper /usr/bin/clickhouse-keeper /usr/bin/clickhouse-keeper
COPY tests/images/zookeeper/config/supervisord.conf /etc/supervisord.conf
COPY --chown=clickhouse:clickhouse tests/images/zookeeper/config/config.xml /etc/clickhouse-keeper/config.xml

COPY dist/*.whl /
RUN python3 -m pip install /*.whl

ENTRYPOINT ["supervisord", "-c", "/etc/supervisord.conf"]
