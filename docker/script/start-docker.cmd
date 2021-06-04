cd ..

docker-compose up --scale spark-worker=2 --scale hdfs-datanode=2 -d

cd script

./uploadFile.cmd