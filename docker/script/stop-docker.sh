#!/bin/bash

sudo docker exec -it hdfs-namenode hdfs dfs -get /results /result

sudo docker kill spark-master spark-worker-1 spark-worker-2 spark-worker-3 spark-worker-4 hdfs-namenode hdfs-datanode-1 hdfs-datanode-2 hdfs-datanode-3 hdfs-datanode-4
sudo docker rm spark-master spark-worker-1 spark-worker-2 spark-worker-3 spark-worker-4 hdfs-namenode hdfs-datanode-1 hdfs-datanode-2 hdfs-datanode-3 hdfs-datanode-4