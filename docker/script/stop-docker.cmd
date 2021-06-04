docker exec -it hdfs-namenode hdfs dfs -get /results /result

docker kill spark-master docker_spark-worker-1 docker_spark-worker-2 hdfs-namenode docker_hdfs-datanode-1 docker_hdfs-datanode-2
docker rm spark-master docker_spark-worker-1 docker_spark-worker-2 hdfs-namenode docker_hdfs-datanode-1 docker_hdfs-datanode-2