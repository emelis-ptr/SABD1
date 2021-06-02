package queries;

import config.HadoopConfig;
import dataset.Schema;
import entity.PuntiSomministrazioneTipologia;
import entity.SomministrazioniVacciniSummaryLatest;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import scala.Tuple2;
import scala.Tuple3;

import java.text.SimpleDateFormat;
import java.util.*;

import static utils.Constants.*;

public class QueryOne {

    private QueryOne() {
    }

    private static final String PATH_HDFS = HadoopConfig.getPathHDFS();

    /**
     ** Query one **
     * Per ogni mese solare e per ciascuna area, calcolare il numero medio di somministrazioni
     * che è stato effettuato giornalmente in un centro vaccinale `generico in quell’area
     * e durante quel mese.
     * Considerare i dati a partire dall’1 Gennaio 2021 fino al 31 Maggio 2021.
     *
     * @param sparkSession:
     * @param sc:
     */
    public static void queryOne(SparkSession sparkSession, JavaSparkContext sc) {
        //Area, numero di centri
        JavaPairRDD<String, Long> pst = getJavaPairPST(sc, sparkSession);
        //Area, Mese, Totale
        JavaPairRDD<String, Tuple2<String, Float>> slsv = getPairSLSV(sparkSession);

        //Join: Area -- (numero centri -- mese, media per giorni)
        // Mese -- ( Area, Media)
        JavaPairRDD<String, Tuple2<String, Float>> finalRes = pst.join(slsv).mapToPair(
                s -> {
                    float avgForCenter = s._2._2._2 / s._2._1;
                    return new Tuple2<>(s._2._2._1, new Tuple2<>(s._1, avgForCenter));
                }).sortByKey(Comparator.naturalOrder());

        Dataset<Row> createSchema = Schema.createSchemaFinalQuery1(sparkSession, finalRes);
        createSchema.coalesce(1).write().mode(SaveMode.Overwrite).option(HEADER, true).format(CSV_FORMAT).save(RES_DIR_QUERY1);
        createSchema.coalesce(1).write().mode(SaveMode.Overwrite).option(HEADER, true).format(CSV_FORMAT).save(PATH_HDFS + DIR_HDFS + RES_DIR_QUERY1);

        createSchema.show(100);
    }

    /**
     * Metodo che prende dal file punti-somministrazione-tipologia il nome della regione e
     * conta quanti centri ci sono per ogni data regione
     *
     * @param sc:
     * @param sparkSession:
     * @return :
     */
    private static JavaPairRDD<String, Long> getJavaPairPST(JavaSparkContext sc, SparkSession sparkSession) {
        JavaRDD<Tuple3<String, String, String>> dataPST = initialDataPST(sparkSession);

        //Nome regione, count centri
        Map<String, Long> count = dataPST.mapToPair(
                s -> new Tuple2<>(s._3(), s._2())).distinct().countByKey();

        List<Tuple2<String, Long>> list = new ArrayList<>();
        for (Map.Entry<String, Long> entry : count.entrySet()) {
            list.add(new Tuple2<>(entry.getKey(), entry.getValue()));
        }

        //Per trasformare in JavaRDD
        return sc.parallelizePairs(list);
    }

    /**
     * Metodo che prende in considerazione dal file somministazione-vaccini-summary-latest i nomi delle regioni,
     * la data e il totale dei vaccini
     *
     * @param sparkSession:
     * @return :
     */
    private static JavaPairRDD<String, Tuple2<String, Float>> getPairSLSV(SparkSession sparkSession) {
        //Area, mese , totale
        JavaRDD<Tuple3<String, String, Integer>> dataSVSL = initialDataSVSL(sparkSession);

        //Area, mese -- media
        return dataSVSL
                .filter(x -> !x._2().equalsIgnoreCase(IGNORE_MONTH) && !x._2().equalsIgnoreCase(IGNORE_MONTH2))
                .mapToPair(s -> new Tuple2<>(new Tuple2<>(s._1(), s._2()), s._3()))
                .reduceByKey(Integer::sum)
                .mapToPair(
                        s -> {
                            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(YEAR_MONTH_FORMAT);
                            Date date = simpleDateFormat.parse(s._1._2);
                            Calendar calendar = Calendar.getInstance();
                            calendar.setTime(date);

                            int days = calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
                            float avgMonth = (float) s._2 / days;
                            return new Tuple2<>(s._1._1, new Tuple2<>(s._1._2, avgMonth));
                        })
                .sortByKey(Comparator.naturalOrder());
    }

    /**
     * Metodo che prende in input solo i dati necessari da svsl
     *
     * @param sparkSession:
     * @return :
     */
    public static JavaRDD<Tuple3<String, String, Integer>> initialDataSVSL(SparkSession sparkSession) {
        /*
        JavaRDD<String> svslFile = sc
                .textFile(PATH_SVSL)
                .sortBy(arg0 -> arg0.split(SPLIT_COMMA)[0], true, 10);  // Prende il file di testo dalla cartella del progetto
                */

        String path = PATH_HDFS + DIR_HDFS + PATH_SVSL;

        JavaRDD<Row> dataset = sparkSession.read()
                .option(HEADER, true)
                .csv(path)
                .javaRDD()
                .sortBy(arg0 -> arg0.getString(0), true, 10);

        JavaRDD<SomministrazioniVacciniSummaryLatest> somministrazioniVacciniSummaryLatestJavaRDD = SomministrazioniVacciniSummaryLatest.getInstance(dataset);

        /**
         //Prende in input i dati dal file di testo
         if(somministrazioniVacciniSummaryLatestJavaRDD.isEmpty()){
         somministrazioniVacciniSummaryLatestJavaRDD =
         svslFile.map(
         ParseCSV::parseCsvSVSL)
         .filter(Objects::nonNull); }
         */

        return somministrazioniVacciniSummaryLatestJavaRDD.map(x -> new Tuple3<>(x.getNomeArea(), x.getMonth(), x.getTotale()));
    }

    /**
     * Metodo che prende in input solo i dati necessari da pst
     *
     * @param sparkSession:
     * @return :
     */
    public static JavaRDD<Tuple3<String, String, String>> initialDataPST(SparkSession sparkSession) {
        /*
        JavaRDD<String> pstFile = sc.textFile(PATH_PST); //Prende il file di testo dalla cartella del progetto
        */

        JavaRDD<Row> dataset = sparkSession.read()
                .option(HEADER, true)
                .csv(PATH_HDFS + DIR_HDFS + PATH_PST)
                .javaRDD();

        JavaRDD<PuntiSomministrazioneTipologia> puntiSomministrazioneTipologiaJavaRDD = PuntiSomministrazioneTipologia.getInstance(dataset);

        /**
         //Prende in input i dati dal file di testo
         if (puntiSomministrazioneTipologiaJavaRDD.isEmpty()) {
         puntiSomministrazioneTipologiaJavaRDD =
         pstFile.map(
         ParseCSV::parseCSVPST)
         .filter(p -> p != null);
         LogFile.infoLog("File from local");}
         */

        return puntiSomministrazioneTipologiaJavaRDD.map(x -> new Tuple3<>
                (x.getArea(), x.getDenominazioneStruttura(), x.getNomeRegione()));
    }


}
