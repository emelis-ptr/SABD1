package queries;

import config.HadoopConfig;
import dataset.Schema;
import dataset.ParseCSV;
import entity.PuntiSomministrazioneTipologia;
import entity.SomministrazioniVacciniSummaryLatest;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import scala.Tuple2;
import scala.Tuple3;
import utils.ComparatorTuple;
import utils.LogFile;

import java.io.IOException;
import java.time.Month;
import java.time.format.TextStyle;
import java.util.*;

import static utils.Constants.*;

public class QueryOne {

    private QueryOne() {
    }

    private static final String PATH_HDFS = HadoopConfig.getPathHDFS();

    /**
     * Query one
     *
     * @param sc:
     */
    public static void queryOne(SparkSession sparkSession, JavaSparkContext sc) throws IOException {
        JavaPairRDD<String, Long> pst = getJavaPairPST(sc, sparkSession);
        JavaPairRDD<String, Tuple2<String, Integer>> slsv = getPairSLSV(sparkSession, sc);

        //Join
        JavaPairRDD<String, Tuple2<Long, Tuple2<String, Integer>>> joined = pst.join(slsv);

        JavaPairRDD<String, Tuple2<String, Float>> finalRes = joined.mapToPair(
                s -> {
                    float avg = (float) s._2._2._2 / s._2._1;
                    String month = Month.of(Integer.parseInt(s._2._2._1)).getDisplayName(TextStyle.FULL, Locale.ITALIAN);
                    return new Tuple2<>(month.toUpperCase(), new Tuple2<>(s._1, avg));
                });

        Dataset<Row> createSchema = Schema.createSchemaFinalQuery1(sparkSession, finalRes);
        createSchema.show(100);
        createSchema.coalesce(1).write().mode(SaveMode.Overwrite).option(HEADER, true).format(CSV_FORMAT).save(RES_DIR_QUERY1);
        createSchema.coalesce(1).write().mode(SaveMode.Overwrite).option(HEADER, true).format(CSV_FORMAT).save(PATH_HDFS + DIR_HDFS + RES_DIR_QUERY1);

    }

    /**
     * Metodo che prende dal file punti-somministrazione-tipologia il nome della regione e
     * conta quanti centri ci sono per ogni data regione
     *
     * @param sc:
     * @return :
     */
    private static JavaPairRDD<String, Long> getJavaPairPST(JavaSparkContext sc, SparkSession sparkSession) throws IOException {
        JavaRDD<Tuple3<String, String, String>> dataPST = initialDataPST(sc, sparkSession);

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
     * @param sc:
     * @return :
     */
    private static JavaPairRDD<String, Tuple2<String, Integer>> getPairSLSV(SparkSession sparkSession, JavaSparkContext sc) {
        JavaRDD<Tuple3<String, String, Integer>> dataSVSL = initialDataSVSL(sparkSession, sc);

        JavaPairRDD<Tuple2<String, String>, Integer> svslPairs = dataSVSL
                .mapToPair(s -> new Tuple2<>(new Tuple2<>(s._1(), s._2()), s._3()))
                .filter(x -> !x._1._2.equalsIgnoreCase(IGNORE_MONTH))
                .reduceByKey(Integer::sum)
                .sortByKey(new ComparatorTuple.Tuple2ComparatorString(), true);

        //Nome regione, Data, Totale
        return svslPairs.mapToPair(
                s -> new Tuple2<>(s._1._1, new Tuple2<>(s._1._2, s._2))).sortByKey();
    }

    /**
     * Metodo che prende in input solo i dati necessari da svsl
     *
     * @param sc:
     * @return :
     */
    public static JavaRDD<Tuple3<String, String, Integer>> initialDataSVSL(SparkSession sparkSession, JavaSparkContext sc) {
        JavaRDD<String> svslFile = sc
                .textFile(PATH_SVSL)
                .sortBy(arg0 -> arg0.split(SPLIT_COMMA)[0], true, 10);

        String path = PATH_HDFS + DIR_HDFS + PATH_SVSL;

        JavaRDD<Row> dataset = sparkSession.read()
                .option(HEADER, true)
                .csv(path)
                .javaRDD()
                .sortBy(arg0 -> arg0.getString(0), true, 10);

        JavaRDD<SomministrazioniVacciniSummaryLatest> somministrazioniVacciniSummaryLatestJavaRDD = SomministrazioniVacciniSummaryLatest.getInstance(dataset);

        if(somministrazioniVacciniSummaryLatestJavaRDD.isEmpty()){
            somministrazioniVacciniSummaryLatestJavaRDD =
                    svslFile.map(
                            ParseCSV::parseCsvSVSL)
                            .filter(Objects::nonNull);
        }

        return somministrazioniVacciniSummaryLatestJavaRDD.map(x -> new Tuple3<>(x.getNomeArea(), x.getMonth(), x.getTotale()));
    }

    /**
     * Metodo che prende in input solo i dati necessari da pst
     *
     * @param sc:
     * @return :
     */
    public static JavaRDD<Tuple3<String, String, String>> initialDataPST(JavaSparkContext sc, SparkSession sparkSession) throws IOException {
        JavaRDD<String> pstFile = sc.textFile(PATH_PST);

        JavaRDD<Row> dataset = sparkSession.read()
                .option(HEADER, true)
                .csv(PATH_HDFS + DIR_HDFS + PATH_PST)
                .javaRDD();

        JavaRDD<PuntiSomministrazioneTipologia> puntiSomministrazioneTipologiaJavaRDD = PuntiSomministrazioneTipologia.getInstance(dataset);

         if(puntiSomministrazioneTipologiaJavaRDD.isEmpty()){
            puntiSomministrazioneTipologiaJavaRDD =
                    pstFile.map(
                            ParseCSV::parseCSVPST)
                            .filter(p -> p != null);
             LogFile.infoLog("File from local");
        }

        return puntiSomministrazioneTipologiaJavaRDD.map(x -> new Tuple3<>
                (x.getArea(), x.getDenominazioneStruttura(), x.getNomeRegione()));
    }


}
