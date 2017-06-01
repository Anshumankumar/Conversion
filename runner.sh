#!/bin/bash
command="/opt/spark-2.0.0-bin-hadoop2.6//bin/spark-submit --master spark://${HOSTNAME}:7077 --class Main \
--executor-memory 28G \
 --driver-memory 4G \
 target/conversion-1.0-SNAPSHOT.jar spark://${HOSTNAME}:7077 "
echo $command
nohup $command > /lrdata/log_conversion 2>&1 &
