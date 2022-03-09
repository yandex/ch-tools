# vim:set ft=dockerfile:
FROM registry.yandex.net/ubuntu:xenial

RUN echo "deb http://mirror.yandex.ru/ubuntu/ xenial main restricted universe multiverse" > /etc/apt/sources.list && \
    echo "deb http://mirror.yandex.ru/ubuntu/ xenial-updates main restricted universe multiverse" >> /etc/apt/sources.list && \
    echo "deb http://mirror.yandex.ru/ubuntu/ xenial-backports main restricted universe multiverse" >> /etc/apt/sources.list && \
    echo "deb http://mirror.yandex.ru/ubuntu/ xenial-security main restricted universe multiverse" >> /etc/apt/sources.list

ENV DEBIAN_FRONTEND noninteractive
ENV LANG en_US.utf8

# Configure apt and locale
COPY ./config/apt/ /var/lib/dist/config/apt
RUN cp --force --recursive --verbose \
    /var/lib/dist/config/apt/* /etc/apt/ && \
    echo 'en_US.UTF-8 UTF-8' > /etc/locale.gen && \
    locale-gen && \
    apt-get -qq update && \
    \
    apt-get -y install \
    supervisor=3.3.1-1yandex \
    openssl git openssh-client \
    python3-pip \
    python3-setuptools \
    python3-dev \
    libffi-dev \
    build-essential \
    netcat-openbsd \
    iputils-ping \
    net-tools \
    dnsutils \
    nginx=1.8.1-1.yandex.16 \
    nginx-full=1.8.1-1.yandex.16 \
    nginx-common=1.8.1-1.yandex.16

RUN mkdir -p /opt/yandex /config

# This config directory is never mounted, and thus should be copied.
COPY ./config/supervisor /etc/supervisor
RUN pip3 install --upgrade pip==9.0.3
# Almost all mocks have custom nginx conf.
RUN ln -s /config/nginx.conf /etc/nginx/nginx.conf

EXPOSE 80 443

CMD ["supervisord", "-c", "/etc/supervisor/supervisord.conf"]
