#!/bin/bash
# Basically, a cannibalized version of ubuntu upstart script 
# + contents of /etc/zookeeper/conf/environment
ulimit -n 8196

NAME=zookeeper
JMXDISABLE="yes, please"
ZOOCFGDIR=/etc/$NAME/conf

# TODO this is really ugly
# How to find out, which jars are needed?
# seems, that log4j requires the log4j.properties file to be in the classpath
CLASSPATH="$ZOOCFGDIR:/usr/share/java/jline.jar:/usr/share/java/log4j-1.2.jar:/usr/share/java/xercesImpl.jar:/usr/share/java/xmlParserAPIs.jar:/usr/share/java/netty.jar:/usr/share/java/slf4j-api.jar:/usr/share/java/slf4j-log4j12.jar:/usr/share/java/zookeeper.jar"

EXEC_OVERRIDES="-Dzookeeper.forceSync=no \
    -Djute.maxbuffer=16777216 \
    -Dzookeeper.snapCount=10000"

ZOOCFG="$ZOOCFGDIR/zoo.cfg"
ZOO_LOG_DIR=/var/log/$NAME
USER=$NAME
GROUP=$NAME
PIDDIR=/var/run/$NAME
PIDFILE=$PIDDIR/$NAME.pid
JAVA=/usr/bin/java
ZOOMAIN="org.apache.zookeeper.server.quorum.QuorumPeerMain"
ZOO_LOG4J_PROP="INFO,ROLLINGFILE"
JMXLOCALONLY="true"
JAVA_OPTS="-XX:+UseG1GC -Xmx256M -XX:+PrintGCDateStamps -Xloggc:/var/log/zookeeper/gc.log -Djava.net.preferIPv6Addresses=true -Djava.net.preferIPv4Stack=false ${EXEC_OVERRIDES}"


[ -r "/usr/share/java/zookeeper.jar" ] || exit 0
[ -d $ZOO_LOG_DIR ] || mkdir -p $ZOO_LOG_DIR
chown $USER:$GROUP $ZOO_LOG_DIR

[ -r /etc/default/zookeeper ] && . /etc/default/zookeeper
if [ -z "$JMXDISABLE" ]; then
    JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.local.only=$JMXLOCALONLY"
fi
# Set "myid" with a number from hostname: zookeeper01.domain -> 1
myid=$(hostname -f | awk -F '.' '{printf("%s", $1)}' | tail -c 1)
datadir=$(awk -F'=' '/dataDir/ {print $2}' "${ZOOCFG}")
[ -z ${myid} ] && exit 1
echo ${myid} > /etc/zookeeper/conf/myid
rm -f ${datadir}/myid
ln -s /etc/zookeeper/conf/myid ${datadir}/myid

# Start process. ZK`s main class never detaches, so start-stop stays in foreground.
exec start-stop-daemon --start -c $USER --exec $JAVA --name zookeeper -- \
     -cp $CLASSPATH $JAVA_OPTS -Dzookeeper.log.dir=${ZOO_LOG_DIR} \
     -Dzookeeper.root.logger=${ZOO_LOG4J_PROP} $ZOOMAIN $ZOOCFG
