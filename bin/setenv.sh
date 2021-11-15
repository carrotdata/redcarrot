#!/usr/bin/env bash

export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-11.0.11.jdk/Contents/Home
export RELEASE=carrot-0.3-SNAPSHOT
export DISTRIBUTION=${RELEASE}.tar.gz
export APPS_PARAMS="conf/carrot-redis.conf"
export INSTANCE_NAME=UAT1_$(pwd)
export APP_OPTS="-Dlocation=${INSTANCE_NAME} -Dlog4j.configurationFile=conf/log4j2.xml"
