ARG PYTHON_VERSION=3.10
FROM python:${PYTHON_VERSION}-bullseye

ARG CLICKHOUSE_VERSION=latest
ENV TZ=Europe/Moscow

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && \
    apt-get update -qq && \
    apt-get upgrade -y && \
    apt-get install -y \
        apt-transport-https \
        ca-certificates \
        locales \
        supervisor \
        git \
        sudo \
        tzdata && \
    echo 'en_US.UTF-8 UTF-8' > /etc/locale.gen && \
    locale-gen

ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8

# Install ClickHouse
COPY tests/images/clickhouse/config/clickhouse-keyring.gpg /usr/share/keyrings/clickhouse-keyring.gpg
RUN mkdir -p /etc/apt/sources.list.d && \
    echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg] https://packages.clickhouse.com/deb stable main" | tee /etc/apt/sources.list.d/clickhouse.list && \
    apt-get update -qq && \
    if [ "${CLICKHOUSE_VERSION}" = "latest" ]; then \
        DEBIAN_FRONTEND=noninteractive apt-get install -y \
            clickhouse-server \
            clickhouse-client \
            clickhouse-common-static; \
    else \
        DEBIAN_FRONTEND=noninteractive apt-get install -y \
            clickhouse-server=${CLICKHOUSE_VERSION} \
            clickhouse-client=${CLICKHOUSE_VERSION} \
            clickhouse-common-static=${CLICKHOUSE_VERSION}; \
    fi && \
    rm -rf /var/lib/apt/lists/* /var/cache/debconf && \
    apt-get clean

COPY tests/images/clickhouse/config/monitor-ch-backup /etc/sudoers.d/monitor-ch-backup
COPY tests/images/clickhouse/config/regions_names_ru.txt /opt/geo/regions_names_ru.txt
COPY tests/images/clickhouse/config/regions_hierarchy.txt /opt/geo/regions_hierarchy.txt

RUN rm -rf /etc/supervisor && \
    ln -s /config/supervisor /etc/supervisor && \
    mkdir /var/log/monrun && \
    ln -s /config/dbaas.conf /etc/dbaas.conf && \
    mkdir -p /etc/yandex/ch-backup && \
    ln -s /config/ch-backup.conf /etc/yandex/ch-backup/ch-backup.conf && \
    ln -s /config/config.xml /etc/clickhouse-server/config.d/ && \
    rm /etc/clickhouse-server/users.xml && \
    ln -s /config/users.xml /etc/clickhouse-server/ && \
    \
    mkdir -p /etc/clickhouse-server/ssl && \
    openssl rand -writerand /root/.rnd && \
    openssl req -subj "/CN=localhost" -new -newkey rsa:2048 -days 365 -nodes -x509 \
        -keyout /etc/clickhouse-server/ssl/server.key \
        -addext "subjectAltName=DNS:clickhouse01,DNS:clickhouse02,DNS:clickhouse01.ch_tools_test,DNS:clickhouse02.ch_tools_test" \
        -out /etc/clickhouse-server/ssl/server.crt && \
    ln -s /etc/clickhouse-server/ssl/server.crt /etc/clickhouse-server/ssl/allCAs.pem && \
    \
    groupadd monitor && useradd -g monitor -G monitor,clickhouse monitor && \
    echo 'monitor ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers && \
    mkdir /var/log/clickhouse-monitoring && \
    chown monitor:monitor -R /var/log/clickhouse-monitoring && \
    mkdir /var/log/keeper-monitoring && \
    chown monitor:monitor -R /var/log/keeper-monitoring && \
    chmod 755 /var/lib/clickhouse && \
    chmod 777 /etc && \
    chmod 755 /etc/clickhouse-server && \
    chmod 750 /etc/clickhouse-server/ssl && \
    chmod 640 /etc/clickhouse-server/ssl/server.crt && \
    chmod 640 /etc/clickhouse-server/ssl/allCAs.pem && \
    chown -R clickhouse:clickhouse /etc/clickhouse-server/ /usr/bin/clickhouse && \
    chmod ugo+Xrw -R /etc/clickhouse-server /etc/clickhouse-client

COPY dist/*.whl /
RUN pip3 install *.whl

# Install ch-backup
# version.txt file is required to install ch-backup from pip
ENV CH_BACKUP_LOCAL_VERSION=2.700.111111111
RUN git clone https://github.com/yandex/ch-backup && \
    echo ${CH_BACKUP_LOCAL_VERSION} | tee ch-backup/ch_backup/version.txt && \
    pip install ./ch-backup

EXPOSE 8123 8443 9000 9440

ENTRYPOINT ["supervisord", "-c", "/etc/supervisor/supervisord.conf"]
