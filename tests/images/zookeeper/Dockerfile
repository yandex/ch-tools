FROM ubuntu/zookeeper

RUN DEBIAN_FRONTEND=noninteractive apt-get update && apt-get install -y supervisor python3-pip && \
    rm -rf /var/lib/apt/lists/* /var/cache/debconf && \
    apt-get clean

COPY tests/images/zookeeper/config/zookeeper.conf /etc/supervisor/supervisord.conf
COPY tests/images/zookeeper/config/zoo.cfg /etc/zookeeper/conf/zoo.cfg
COPY tests/images/zookeeper/config/log4j.properties /etc/zookeeper/conf/log4j.properties

COPY dist/*.whl /
RUN python3 -m pip install *.whl

ENTRYPOINT ["supervisord", "-c", "/etc/supervisor/supervisord.conf"]
