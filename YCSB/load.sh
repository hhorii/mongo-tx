#!/bin/bash

MAVEN_DIR=~/.m2/repository

SH_DIR=`dirname $0`

cd $SH_DIR

./run.sh -load -p ycsb.db.drop=true