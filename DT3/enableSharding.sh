#!/bin/sh

URI=$1

mongo $URI << EOS
var sys = require('sys');
use DT3;
db.dropDatabase();

use DT3;
sh.enableSharding("DT3");
sh.shardCollection("DT3.HOLDING", { ACCOUNT_ACCOUNT_ID:"hashed" });
sh.shardCollection("DT3.ACCOUNTPROFILE", { _id:"hashed" });
sh.shardCollection("DT3.ACCOUNT", { PROFILE_USERID:"hashed" });
sh.shardCollection("DT3.ORDER", { ACCOUNT_ACCOUNTID:"hashed" });
sh.shardCollection("DT3.DT3_IDX", { _id:"hashed" });
EOS
