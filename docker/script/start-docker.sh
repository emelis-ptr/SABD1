#!/bin/bash

cd ..

docker-compose up --scale spark-worker=3 --scale hdfs-datanode=4 -d

cd script

./uploadFile.cmd

cp target\Progetto-1.0-SNAPSHOT.jar docker\data\spark_data

sudo docker exec spark-master /bin/bash -c "spark-submit --class queries.Main --master "spark://spark-master:7077" /spark_data/Progetto-1.0-SNAPSHOT.jar