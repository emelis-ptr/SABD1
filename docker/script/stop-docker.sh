#!/bin/bash

sudo docker kill docker_spark-worker_1 docker_spark-worker_2 spark-master docker_hdfs-datanode_1 docker_hdfs-datanode_2 hdfs-namenode
sudo docker rm docker_spark-worker_1 docker_spark-worker_2 spark-master docker_hdfs-datanode_1 docker_hdfs-datanode_2 hdfs-namenode