#!/bin/bash
cd ..
cd ..

cp target\Progetto-1.0-SNAPSHOT.jar docker\data\spark_data

sudo docker exec spark-master /bin/bash -c "spark-submit" --class queries.Main --master "spark://spark-master:7077" /spark_data/Progetto-1.0-SNAPSHOT.jar