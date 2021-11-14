#!/usr/bin/env bash

export APP_PORT=6379
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export RELEASE=carrot-1.0.0
export DISTRIBUTION=${RELEASE}.tar.gz
export HEALTH_MONITOR=http://localhost:${APP_PORT}/ping
export APPS_PARAMS="conf/carrot-redis.conf"
export INSTANCE_NAME=UAT1_$(pwd)
export APP_OPTS="-Dlocation=${INSTANCE_NAME} -Dlog4j.configurationFile=conf/log4j2.xml"
