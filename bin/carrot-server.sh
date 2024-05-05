#!/usr/bin/env bash

START_HOME=$PWD
# within instruction $(dirname "$(realpath "$0")") you can start server from any directory. No just from bin
# for example, you can start app by root from cron. like: /bin/carrot/bin/carrot-server.sh reboot
# it is important if you use auto-start script in case of server reboot.
#START_HOME=$(dirname "$(realpath "$0")")
echo Carrot server home directory is "${START_HOME}"

cd "${START_HOME}" || exit

. ./setenv.sh

libdir="${START_HOME}/../lib/${RELEASE}"
rm -rf "${libdir}"
mkdir -p "${libdir}"
cd "${libdir}" || exit 1
tar zxf "${START_HOME}/../dist/target/${DISTRIBUTION}" &>/dev/null
cd ../..
for ix in $(find "${libdir}"); do
  CPATH=${ix}\:${CPATH}
done

export JVM_OPTS="--add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED \
	--add-opens java.base/java.security=ALL-UNNAMED --add-opens jdk.unsupported/sun.misc=ALL-UNNAMED \
	--add-opens java.base/sun.security.action=ALL-UNNAMED --add-opens jdk.naming.rmi/com.sun.jndi.rmi.registry=ALL-UNNAMED \
	--add-opens java.base/sun.net=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED -cp .:${CPATH} ${APP_OPTS}"

#===== find pid =====
# shellcheck disable=SC2120
pid() {
  echo "$(ps -aef | grep "${INSTANCE_NAME}" | grep -v grep | awk {'print $2'})"
}

#===== start carrot =====
start() {
  PID=$(pid)
  if [ ! -z "${PID}" ]; then
    echo Server and port number in use PID="${PID}"
    exit 1
  fi

  exec_cmd="${JAVA_HOME}/bin/java ${JVM_OPTS} org.bigbase.carrot.redis.CarrotMain ${APPS_PARAMS} start"
  echo "${exec_cmd}"
  mkdir -p logs
  nohup ${exec_cmd} >>logs/carrot-stdout.log &
  echo "Carrot instance ${INSTANCE_NAME} is staring on PID ${PID}, please wait..."

  sleep 1

  PID=$(pid)
  if [ ! -z "${PID}" ]; then
    echo "Carrot instance ${INSTANCE_NAME} successfully started. PID ${PID}"
    exit 0
  fi

  echo "Carrot instance failed to start on instance name ${INSTANCE_NAME}"
  exit 1
}

#==== stop carrot ====
stop() {
  if_continue=$1

  PID=$(pid)
  if [ ! -z "${PID}" ]; then
    exec_cmd="${JAVA_HOME}/bin/java ${JVM_OPTS} org.bigbase.carrot.redis.CarrotMain ${APPS_PARAMS} stop"   
    nohup ${exec_cmd} &
    echo "Carrot instance ${INSTANCE_NAME} is terminating on PID ${PID}, please wait..."
    sleep 3
  fi

  sleep 1

  PID=$(pid)
  if [ ! -z "${PID}" ]; then
    echo "Carrot server still running on instance ${INSTANCE_NAME} and can't be stopped for some reason. PID ${PID}"
  else
    echo "No instances of Carrot server are runnning"
  fi

  if [ -z "${if_continue}" ]; then
    exit 0
  fi
}

#===== reboot =====
reboot() {
  stop 1
  sleep 2
  start
}

#===== usage =====
usage() {
  echo
  echo Usage:
  # shellcheck disable=SC2102
  echo \$\> \./carrot-server.sh [start]\|[stop]\|[reboot]
  echo
}

#==== main =====
${JAVA_HOME}/bin/java -version
cmd=$1
# shellcheck disable=SC1009
if [ "${cmd}" == "start" ]; then
  start
elif [ "${cmd}" == "stop" ]; then
  stop
elif [ "${cmd}" == "reboot" ]; then
  reboot
else
  usage
fi
