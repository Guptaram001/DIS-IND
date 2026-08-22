#!/bin/sh
set -eu

# Scaled Compose replicas cannot share a fixed hostname. Advertise the
# replica's Docker-network IP, which is unique and reachable for the run.
: "${AKKA_HOSTNAME:=$(hostname -i | awk '{print $1}')}"
: "${AKKA_NODE_ID:=$(hostname)}"
: "${AKKA_PORT:=2551}"
: "${AKKA_SEED_HOST:=coordinator}"
: "${AKKA_SEED_PORT:=2551}"
: "${DIS_IND_NODE_ROLE:=worker}"
: "${JAVA_XMS:=512m}"
: "${JAVA_XMX:=2g}"
: "${DIS_IND_DIAGNOSTICS_DIR:=/data/diagnostics}"
: "${DIS_IND_MAIN_CLASS:=disIND.DisINDMain}"

export AKKA_HOSTNAME AKKA_PORT

#Adding Ports to view through jconsole
JMX_OPTIONS=""

if [ -n "${JMX_PORT:-}" ]; then
  JMX_OPTIONS="
    -Dcom.sun.management.jmxremote
    -Dcom.sun.management.jmxremote.port=${JMX_PORT}
    -Dcom.sun.management.jmxremote.rmi.port=${JMX_PORT}
    -Dcom.sun.management.jmxremote.authenticate=false
    -Dcom.sun.management.jmxremote.ssl=false
    -Dcom.sun.management.jmxremote.local.only=false
    -Djava.rmi.server.hostname=127.0.0.1
  "
fi

exec java \
  "-Xms${JAVA_XMS}" \
  "-Xmx${JAVA_XMX}" \
  "-XX:+HeapDumpOnOutOfMemoryError" \
  "-XX:HeapDumpPath=${DIS_IND_DIAGNOSTICS_DIR}/heap-${AKKA_NODE_ID}.hprof" \
  "-XX:ErrorFile=${DIS_IND_DIAGNOSTICS_DIR}/hs-err-${AKKA_NODE_ID}-%p.log" \
  "-XX:NativeMemoryTracking=summary" \
  "-Xlog:gc*,safepoint:file=${DIS_IND_DIAGNOSTICS_DIR}/gc-${AKKA_NODE_ID}.log:time,uptime,level,tags:filecount=3,filesize=50M" \
  "-XX:StartFlightRecording=name=disIND,settings=profile,filename=${DIS_IND_DIAGNOSTICS_DIR}/${AKKA_NODE_ID}.jfr,disk=true,dumponexit=true,maxsize=512m" \
  "-Dakka.cluster.roles.0=${DIS_IND_NODE_ROLE}" \
  "-Dakka.cluster.seed-nodes.0=akka://disIND@${AKKA_SEED_HOST}:${AKKA_SEED_PORT}" \
  ${JMX_OPTIONS} \
  ${JAVA_OPTS:-} \
  -cp /opt/dis-ind/dis-ind.jar \
  "${DIS_IND_MAIN_CLASS}" \
  "$@"
