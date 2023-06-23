FROM ubuntu/zookeeper
COPY staging/tools/ /tmp/tools/

RUN DEBIAN_FRONTEND=noninteractive apt-get update && apt-get install -y supervisor && \
    rm -rf /var/lib/apt/lists/* /var/cache/debconf && \
    apt-get clean

RUN tar -x -f /tmp/tools/mdb-ch-tools.*.tar.gz -C /

COPY images/zookeeper/config/zookeeper.conf /etc/supervisor/supervisord.conf
COPY images/zookeeper/config/zoo.cfg /etc/zookeeper/conf/zoo.cfg
COPY images/zookeeper/config/log4j.properties /etc/zookeeper/conf/log4j.properties

ENTRYPOINT ["supervisord", "-c", "/etc/supervisor/supervisord.conf"]
