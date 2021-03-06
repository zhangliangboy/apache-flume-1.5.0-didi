#!/bin/bash 
cd /home/zhangliang/work/tool;
rm /home/zhangliang/work/tool/error_api.log;
echo "collecting...";

for i in `cat /home/zhangliang/work/tool/servers_api.txt `;
 do ssh $i "cat /app/logs/error.log" ;
done>>/home/zhangliang/work/tool/error_api.log;

echo "analyzing...";

BASE_DIR=$(cd $(dirname $0)/;pwd)
CLASSPATH=$BASE_DIR/classes:$BASE_DIR/config:$BASE_DIR
for jar in $BASE_DIR/lib/*.jar
do
  CLASSPATH="$CLASSPATH:$jar"
done
/usr/local/java/bin/java -cp $CLASSPATH com.kuaidadi.ErrorToolLauncher error_api.log;