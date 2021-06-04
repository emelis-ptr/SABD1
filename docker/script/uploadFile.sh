#!/bin/bash

cd ..
cd ..

sudo docker cp data hdfs-namenode:/data/
sudo docker exec -it hdfs-namenode hdfs dfs -put /data /data
sudo docker exec -it hdfs-namenode hdfs dfs -mkdir /results
sudo docker exec -it hdfs-namenode hdfs dfs -chmod 0777 /results

# shellcheck disable=SC2164
cd docker
# shellcheck disable=SC2164
cd script

./submit.sh