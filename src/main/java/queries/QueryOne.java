package queries;

import dataset.Schema;
import entity.PuntiSomministrazioneTipologia;
import entity.SomministrazioniVacciniSummaryLatest;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import scala.Tuple2;
import scala.Tuple3;
import utils.LogFile;

import java.text.SimpleDateFormat;
import java.util.*;

import static utils.Constants.*;

public class QueryOne {

    private QueryOne() {
    }

    /**
     * * Query one **
     * Per ogni mese solare e per ciascuna area, calcolare il numero medio di somministrazioni
     * che è stato effettuato giornalmente in un centro vaccinale `generico in quell’area
     * e durante quel mese.
     * Considerare i dati a partire dall’1 Gennaio 2021 fino al 31 Maggio 2021.
     *
     * @param sc:
     */
    public static void queryOne(JavaSparkContext sc) {
        //Area, numero di centri
        JavaPairRDD<String, Long> pst = getJavaPairPST(sc);
        //Area, Mese, Totale
        JavaPairRDD<String, Tuple2<String, Float>> slsv = getPairSLSV(sc);

        //Join: Area -- (numero centri -- mese, media per giorni)
        // Mese -- ( Area, Media)
        JavaPairRDD<String, Tuple2<String, Float>> finalRes = pst.join(slsv).mapToPair(
                s -> {
                    float avgForCenter = s._2._2._2 / s._2._1;
                    return new Tuple2<>(s._2._2._1, new Tuple2<>(s._1, avgForCenter));
                }).sortByKey(Comparator.naturalOrder());

        Dataset<Row> createSchema = Schema.createSchemaFinalQuery1(sc, finalRes);
        createSchema.coalesce(1).write().mode(SaveMode.Overwrite).option(HEADER, true).format(CSV_FORMAT).save("hdfs://hdfs-namenode:9000/results/queryOne/");

        createSchema.show(100);
    }

    /**
     * Metodo che prende dal file punti-somministrazione-tipologia il nome della regione e
     * conta quanti centri ci sono per ogni data regione
     *
     * @param sc:
     * @return :
     */
    private static JavaPairRDD<String, Long> getJavaPairPST(JavaSparkContext sc) {
        JavaRDD<Tuple3<String, String, String>> dataPST = initialDataPST(sc);

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
     * @param sc :
     * @return :
     */
    private static JavaPairRDD<String, Tuple2<String, Float>> getPairSLSV(JavaSparkContext sc) {
        //Area, mese , totale
        JavaRDD<Tuple3<String, String, Integer>> dataSVSL = initialDataSVSL(sc);

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
     * @param sc :
     * @return :
     */
    public static JavaRDD<Tuple3<String, String, Integer>> initialDataSVSL(JavaSparkContext sc) {

        JavaRDD<String> data = sc.textFile("hdfs://hdfs-namenode:9000/data/data/somministrazioni-vaccini-summary-latest.csv")
                .sortBy(arg0 -> arg0.split(SPLIT_COMMA)[0], true, 10);

        JavaRDD<SomministrazioniVacciniSummaryLatest> somministrazioniVacciniSummaryLatestJavaRDD = SomministrazioniVacciniSummaryLatest.getInstance(data);

        return somministrazioniVacciniSummaryLatestJavaRDD.map(x -> new Tuple3<>(x.getNomeArea(), x.getMonth(), x.getTotale()));
    }

    /**
     * Metodo che prende in input solo i dati necessari da pst
     *
     * @param sc:
     * @return :
     */
    public static JavaRDD<Tuple3<String, String, String>> initialDataPST(JavaSparkContext sc) {

        JavaRDD<String> data = sc.textFile("hdfs://hdfs-namenode:9000/data/data/punti-somministrazione-tipologia.csv");

        JavaRDD<PuntiSomministrazioneTipologia> puntiSomministrazioneTipologiaJavaRDD = PuntiSomministrazioneTipologia.getInstance(data);

        return puntiSomministrazioneTipologiaJavaRDD.map(x -> new Tuple3<>
                (x.getArea(), x.getDenominazioneStruttura(), x.getNomeRegione()));
    }


}
