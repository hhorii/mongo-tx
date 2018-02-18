#!/bin/bash

MAVEN_DIR=~/.m2/repository

SH_DIR=`dirname $0`

cd $SH_DIR

source ycsb_env.sh

java -classpath $CP $CLASS $YCSB_ARGS $*
