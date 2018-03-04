#!/bin/bash

MAVEN_DIR=~/.m2/repository
URI=mongodb://localhost:27017/DT3

SH_DIR=`dirname $0`

cd $SH_DIR

source dt3_env.sh

DT3_ARGS="$DT3_ARGS -Dmongodb.url=$URI"
#DT3_ARGS="$DT3_ARGS -Dmongotx.usesecondary=true"

java -classpath $CP $DT3_ARGS $* $CLASS
