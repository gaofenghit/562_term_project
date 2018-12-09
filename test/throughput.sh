#!/bin/bash
  

bucketname="562.tp.jxf"
filename="500000.csv"


getThrough() {
  aws lambda update-function-configuration --function-name test_throughput --memory-size $1 2>/dev/null 1>/dev/null
  json={"\"bucketname\"":\"${bucketname}\"","\"filename\"":\"${filename}\""}
  output=`curl -s -H "Content-Type: application/json" -X POST -d $json https://isvm7mme74.execute-api.us-east-1.amazonaws.com/test_throughput | jq -r ".throughput"`
  echo "${1}:${output}"
}

for((i=768;i<=3072;i=i+64));  
do
  getThrough $i
done  


