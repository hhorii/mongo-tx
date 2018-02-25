#!/bin/sh

URI=$1

mongo $URI << EOS
var sys = require('sys');
use YCSB;
sh.enableSharding("YCSB");
sh.shardCollection("YCSB.usertable", { _id:1 });
sh.shardCollection("YCSB.YCSB_INDEX", { _id:"hashed"});

use admin;
for (var i=0; i<1000000; i+=1000) {
  print(i);
  printjson(db.runCommand( { split : "YCSB.usertable" , middle : { _id : i } } ));
}
EOS
