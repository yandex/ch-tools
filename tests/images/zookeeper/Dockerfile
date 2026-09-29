ARG CLICKHOUSE_VERSION=latest
FROM clickhouse/clickhouse-keeper:${CLICKHOUSE_VERSION}-alpine AS keeper

FROM alpine:3.22 AS certificates

RUN apk add --no-cache openssl

COPY tests/images/ssl/ /certificates/
COPY tests/staging/images/{{ instance_name }}/ssl/server.ext /certificates/

RUN openssl genrsa -out /certificates/server.key 2048 && \
    openssl req -new -key /certificates/server.key \
        -out /certificates/server.csr \
        -subj "/CN=localhost" && \
    openssl x509 -req -in /certificates/server.csr \
        -CA /certificates/rootCACert.pem \
        -CAkey /certificates/rootCAKey.pem \
        -CAcreateserial -out /certificates/server.crt \
        -days 365 -sha256 -extfile /certificates/server.ext

FROM python:3.10-bookworm

ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8

RUN apt-get update -qq && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y locales netcat-openbsd sudo supervisor && \
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
COPY --from=certificates --chown=clickhouse:clickhouse \
    /certificates/server.crt \
    /certificates/server.key \
    /etc/clickhouse-keeper/
COPY --from=certificates /certificates/rootCACert.pem /usr/local/share/ca-certificates/ch-tools-test-ca.crt
RUN update-ca-certificates

COPY dist/*.whl /
RUN python3 -m pip install /*.whl

ENTRYPOINT ["supervisord", "-c", "/etc/supervisord.conf"]
