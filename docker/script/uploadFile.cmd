cd ..
cd ..

docker cp data hdfs-namenode:/data/
docker exec -it hdfs-namenode hdfs dfs -put /data /data
docker exec -it hdfs-namenode hdfs dfs -mkdir /results
docker exec -it hdfs-namenode hdfs dfs -chmod 0777 /results

cd docker/script

./submit.cmd