#!/bin/bash
  

bucketname="562.tp.jxf"
filename="50000.csv"


agg="Max(UnitsSold), total(TotalRevenue)"
filter="Region='Australia and Oceania' and ItemType='Office Supplies'"

agg="Max(UnitsSold), total(TotalRevenue), min(OrderProcessingTime)"



echo $agg
echo $filter

agg=${agg//\ /\\u0020}
filter=${filter//\ /\\u0020}


json={"\"flag\"":"\"1\"","\"bucketname\"":\"${bucketname}\"","\"filename\"":\"${filename}\"","\"aggregation\"":"\"$agg\"","\"filter\"":"\"$filter\""}
#echo $json
output=`time curl -s -H "Content-Type: application/json" -X POST -d $json https://69pca5i95d.execute-api.us-east-1.amazonaws.com/test_tp | jq -r ".value"`
echo "A:$output"


json={"\"flag\"":"\"2\"","\"bucketname\"":\"${bucketname}\"","\"filename\"":\"${filename}\"","\"aggregation\"":"\"$agg\"","\"filter\"":"\"$filter\""}
#echo $json
output=`time curl -s -H "Content-Type: application/json" -X POST -d $json https://69pca5i95d.execute-api.us-east-1.amazonaws.com/test_tp | jq -r ".value"`
echo "B:$output"


json={"\"flag\"":"\"3\"","\"bucketname\"":\"${bucketname}\"","\"filename\"":\"${filename}\"","\"aggregation\"":"\"$agg\"","\"filter\"":"\"$filter\""}
#echo "C:$json"
output=`time curl -s -H "Content-Type: application/json" -X POST -d $json https://69pca5i95d.execute-api.us-east-1.amazonaws.com/test_tp | jq -r ".value"`
echo "C:$output"



#time output=`aws lambda invoke --invocation-type RequestResponse --function-name test_fp --region us-east-1 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`
#echo $output



