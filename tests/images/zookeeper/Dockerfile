FROM ch-tools-tests-base

ENV ZK_VERSION=3.4.8-1

RUN apt-get update -qq && apt-get install -y \
        openjdk-8-jre-headless \
        zookeeper=${ZK_VERSION} \
        zookeeper-bin=${ZK_VERSION} \
        libzookeeper-java=${ZK_VERSION}

COPY images/zookeeper/config/zookeeper.conf /etc/supervisor/conf.d/zookeeper.conf
COPY images/zookeeper/config/start_zk.sh /opt/yandex/start_zk.sh
COPY images/zookeeper/config/zoo.cfg /etc/zookeeper/conf/zoo.cfg
COPY images/zookeeper/config/log4j.properties /etc/zookeeper/conf/log4j.properties

EXPOSE 2181 2281
