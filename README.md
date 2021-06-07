# SABD-Project One

### *`Dataset`*

I file csv sono stati scaricati dal [https://github.com/italia/covid19-opendata-vaccini/tree/master/dati][repository covid-19] 
e sono stati salvati all'interno della directory /data. La data dei file utilizzati risalgono al 1 Giugno 2021.

### *`Prerequisiti`*

Il progetto è stato eseguito in un ambiente Windows.
Bisogna installare docker:
- [[https://docs.docker.com/docker-for-windows/install/][Docker per Windows]]
- E' necessario aver installato:
    - maven
    - java

### *`Istruzioni`*

 1. Eseguire _`maven package `_ per la creazione del file
    jar. `_~~Il file verrà creato all'interno della cartella target~~_`. 

 2. E' stato creato un file docker-compose.yml per la creazione dei container spark e hdfs. 
    Per eseguirlo bisogna far partire lo script `start-docker` all'interno 
    della directory docker/script.
     - Verranno creati 3 spark-worker e hdfs-datanode
     - Verranno caricati i file csv su hdfs
     - Verrà eseguito il comando 
        - `docker exec spark-master /bin/bash -c "spark-submit --class queries.Main --master "spark://spark-master:7077" /spark_data/Progetto-1.0-SNAPSHOT.jar`
     - Terminata l'esecuzione dello script precedente, i risultati verranno salvati su HDFS. 
       Ma è possibile trovarli anche all'interno del progetto nella directory /results
       
 4. E' possibile consultare i risultati su hdfs tramite il comando: 
    `docker exec -it hdfs-namenode hdfs dfs -cat [percorso/nomefile]`
    
 3. Per rimuovere i container eseguire `stop-docker`

### *`Queries`*

**_Query One:_** 
 - Utilizzando somministrazioni-vaccini-summary-latest.csv e
punti-somministrazione-tipologia.csv, per ogni mese solare e per ciascuna area, calcolare il numero medio di somministrazioni che è stato effettuato giornalmente in un centro vaccinale generico in quell’area e durante quel mese. Considerare i dati a partire dall’1 Gennaio 2021.


**_Query Two:_** 
- Utilizzando somministrazioni-vaccini-latest.csv, per le donne, per ogni fascia anagrafica e per ogni mese solare, determinare le prime 5 aree per le quali e previsto il maggior numero di vaccinazioni il primo giorno del mese successivo. Per determinare la classifica mensile e prevedere il numero di vaccinazioni, considerare la retta di regressione che approssima l’andamento delle vaccinazioni giornaliere. Per la risoluzione della query, considerare le sole categorie per cui nel mese solare in esame vengono registrati almeno due giorni di campagna vaccinale. 
Viene inoltre richiesto di calcolare la classifica per ogni mese e categoria a partire dai dati raccolti dall’1 Febbraio 2021.

### *`Framework`*

- [https://spark.apache.org/][Apache Spark]: localhost:8080
- [https://hadoop.apache.org/][Apache Hadoop]: localhost:9870

[Docker per Windows]: https://docs.docker.com/docker-for-windows/install/

[repository covid-19]: https://github.com/italia/covid19-opendata-vaccini/tree/master/dati

[Apache Spark]: https://spark.apache.org/

[Apache Hadoop]: https://hadoop.apache.org/

