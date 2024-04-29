#!/usr/bin/env bash

# Yeah, let set individual JAVA_HOME in .bashrc ?
# JAVA_HOME variable could be set on stand alone not-dev server.
#export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-11.0.11.jdk/Contents/Home
export RELEASE=carrot-0.3-SNAPSHOT
export DISTRIBUTION=${RELEASE}.tar.gz
export APPS_PARAMS="conf/carrot-redis.conf"
export INSTANCE_NAME=DEV_$(pwd)
export APP_OPTS="-Dlocation=${INSTANCE_NAME} -Dlog4j.configurationFile=conf/log4j2.xml"
# Ubuntu jemalloc path
# export LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so
# export MALLOC_CONF=prof:true,lg_prof_interval:30,lg_prof_sample:17
# jeprof --show_bytes --gif /path/to/jvm/bin/java jeprof*.heap > /tmp/app-profiling.gif
# MAc jemalloc path
# export LD_PRELOAD=