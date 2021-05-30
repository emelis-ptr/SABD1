package queries;

import config.HadoopConfig;
import dataset.Schema;
import entity.SomministrazioniVacciniLatest;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import utils.ComparatorTuple;
import utils.Data;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static java.util.Calendar.MONTH;
import static java.util.Calendar.YEAR;
import static utils.Constants.*;

public class QueryTwo {

    private QueryTwo() {
    }

    private static final String PATH_HDFS = HadoopConfig.getPathHDFS();

    /**
     * Query two
     *
     * @param sc:
     * @param spark:
     * @throws ParseException:
     */
    public static void queryTwo(JavaSparkContext sc, SparkSession spark) throws ParseException {
        //Data(anno-mese), area, fascia_anagrafica -- data , num_vacc_femminile
        JavaPairRDD<Tuple3<String, String, String>, Tuple2<Date, Integer>> initialData = initialData(spark, sc);

        //conta per ogni key il numero di giorni in cui sono stati effettutati i vaccini
        //Data(anno-mese), area, fascia_anagrafica -- numDaysToMonth
        Map<Tuple3<String, String, String>, Long> countNumDaysMap = initialData.countByKey();
        JavaPairRDD<Tuple3<String, String, String>, Long> countNumDays = mapToJavaPairRDD(sc, countNumDaysMap);

        //Data(anno-mese), area, fascia_anagrafica
        Broadcast<List<Tuple3<String, String, String>>> filterKeyMinor2 = filterNumDayMinor2(sc, countNumDays);
        //filter
        initialData = initialData.filter(x -> !filterKeyMinor2.value().contains(x._1));
        //groupByKey
        // Data(anno-mese), area, fascia_anagrafica -- data , num_vacc_femminile
        JavaPairRDD<Tuple3<String, String, String>, Iterable<Tuple2<Date, Integer>>> groupInitData = initialData
                .groupByKey()
                .sortByKey(new ComparatorTuple.Tuple3ComparatorString(), true);

        JavaPairRDD<Tuple3<Date, String, Double>, String> simpleRegression = simpleRegression(groupInitData);

        //data, fascia-anagrafica -- area, predizione
        JavaPairRDD<Tuple2<Date, String>, Tuple2<String, Double>> rank = simpleRegression
                .mapToPair(line -> new Tuple2<>(new Tuple2<>(line._1._1(), line._1._2()), new Tuple2<>(line._2, line._1._3())))
                .groupByKey()
                .flatMapToPair(
                        row -> {
                            List<Tuple2<String, Double>> tuple2 = IteratorUtils.toList(row._2.iterator());
                            List<Tuple2<Tuple2<Date, String>, Tuple2<String, Double>>> list = new ArrayList<>();

                            for (Tuple2<String, Double> tuple : tuple2) {
                                list.add(new Tuple2<>(new Tuple2<>(row._1._1(), row._1._2()), new Tuple2<>(tuple._1, tuple._2)));
                            }

                            return list.stream().limit(5).iterator();
                        }
                ).sortByKey(new ComparatorTuple.Tuple2ComparatorDateString(), true);

        Dataset<Row> createSchema = Schema.createSchemaQuery3(spark, rank);
        createSchema.show(50);
        createSchema.coalesce(1).write().mode(SaveMode.Overwrite).option(HEADER, true).format(CSV_FORMAT).save(RES_DIR_QUERY2);
        createSchema.coalesce(1).write().mode(SaveMode.Overwrite).option(HEADER, true).format(CSV_FORMAT).save(PATH_HDFS + DIR_HDFS + RES_DIR_QUERY2);
    }

    /**
     * Metodo che predice il numero di vaccini del primo giorno del mese successivo
     * con simple regression
     *
     * @param lines:
     * @return :
     */
    public static JavaPairRDD<Tuple3<Date, String, Double>, String> simpleRegression(JavaPairRDD<Tuple3<String, String, String>, Iterable<Tuple2<Date, Integer>>> lines) {
        SimpleRegression regression = new SimpleRegression();

        JavaPairRDD<Tuple3<String, String, String>, SimpleRegression> map = lines
                .flatMapToPair(line -> {
                    List<Tuple2<Tuple3<String, String, String>, SimpleRegression>> list = new ArrayList<>();

                    for (Tuple2<Date, Integer> en : line._2) {
                        regression.addData((double) en._1.getTime(), (double) en._2);
                        list.add(new Tuple2<>(line._1, regression));
                    }

                    return list.stream().iterator();

                });

        return map.mapToPair(record -> {
            Date nextMonth2 = Data.getNextMonth(record._1._1()).getTime();
            double predictedVaccinations = record._2.predict((double) (nextMonth2).getTime());

            return new Tuple2<>(new Tuple3<>(nextMonth2, record._1._3(), predictedVaccinations), record._1._2());
        }).distinct().sortByKey(new ComparatorTuple.Tuple3ComparatorDateStringDouble(), true);
    }

    /**
     * Metodo che restituisce una coppia:
     * Data(anno-mese), area, fascia_anagrafica -- data , num_vacc_femminile
     *
     * @param sc:
     * @return :
     */
    private static JavaPairRDD<Tuple3<String, String, String>, Tuple2<Date, Integer>> initialData(SparkSession sparkSession, JavaSparkContext sc) {
        JavaRDD<Tuple4<String, String, String, Integer>> svl = initialDataSVL(sparkSession, sc);

        //Data(anno-mese), area, fascia_anagrafica -- data , num_vacc_femminile
        return svl
                .mapToPair(
                        s -> {
                            //default, ISO_LOCAL_DATE
                            LocalDate localDate = LocalDate.parse(s._1(), DateTimeFormatter.ofPattern(DATE_WITH_DASHES));
                            //Data, area, fascia-anagrafica -- num_vac_femminili
                            return new Tuple2<>(new Tuple3<>(localDate, s._2(), s._3()), s._4());
                        })
                .filter(x -> !x._1._1().isBefore(LocalDate.parse(IGNORE_DATE_BEFORE))) //filtra ignorando tutte le date prima 2021-02-01
                .reduceByKey(Integer::sum)  //somma il numero di vacc-femminile per la stessa chiave (dato che ci sono più tipologie)
                .mapToPair(
                        s -> {
                            String yearMonth = "";
                            Date date1 = null;
                            try {
                                date1 = new SimpleDateFormat(DATE_WITH_DASHES).parse(String.valueOf(s._1._1()));
                                Calendar cal = Calendar.getInstance();
                                cal.setTime(date1);
                                yearMonth = cal.get(YEAR) + "-" + (cal.get(MONTH) + 1);

                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                            //Data(anno-mese), area, fascia_anagrafica -- data , num_vacc_femminile
                            return new Tuple2<>(new Tuple3<>(yearMonth, s._1._2(), s._1._3()), new Tuple2<>(date1, s._2));
                        }
                );
    }

    /**
     * Metodo che filtra considerando una chiave il cui
     * numero dei giorni di vaccinazione sia minore di 2
     *
     * @param sc:
     * @param res:
     * @return :
     */
    private static Broadcast<List<Tuple3<String, String, String>>> filterNumDayMinor2(JavaSparkContext sc, JavaPairRDD<Tuple3<String, String, String>, Long> res) {
        return sc
                .broadcast(res.filter(row -> row._2 < 2)
                        .keys()
                        .collect());
    }

    /**
     * Metodo che converte la mappa in JavaPairRDD
     *
     * @param sc:
     * @param countNumDays:
     * @return :
     */
    private static JavaPairRDD<Tuple3<String, String, String>, Long> mapToJavaPairRDD(JavaSparkContext sc, Map<Tuple3<String, String, String>, Long> countNumDays) {
        List<Tuple2<Tuple3<String, String, String>, Long>> list = new ArrayList<>();
        for (Map.Entry<Tuple3<String, String, String>, Long> entry : countNumDays.entrySet()) {
            list.add(new Tuple2<>(entry.getKey(), entry.getValue()));
        }

        //Per trasformare in JavaRDD
        return sc.parallelizePairs(list);
    }

    /**
     * Metodo che prende in input solo i dati necessari da svl
     *
     * @param sc:
     * @return :
     */
    public static JavaRDD<Tuple4<String, String, String, Integer>> initialDataSVL(SparkSession sparkSession, JavaSparkContext sc) {
        JavaRDD<String> svlFile = sc
                .textFile(PATH_SVL)
                .sortBy(arg0 -> arg0.split(SPLIT_COMMA)[0], true, 10);

        JavaRDD<Row> dataset = sparkSession.read()
                .option("header", true)
                .csv(PATH_HDFS + "/sabd/" + PATH_SVL)
                .javaRDD();

        //Data(anno-mese), area, fascia_anagrafica -- data , num_vacc_femminile
        JavaRDD<SomministrazioniVacciniLatest> somministrazioniVacciniLatestJavaRDD = SomministrazioniVacciniLatest.getInstance(dataset);


        /*if (somministrazioniVacciniLatestJavaRDD.isEmpty()) {
            somministrazioniVacciniLatestJavaRDD =
                    svlFile.map(
                            ParseCSV::parseCsvSVL)
                            .filter(Objects::nonNull);
        }*/

        return somministrazioniVacciniLatestJavaRDD.map(x -> new Tuple4<>(x.getDataSomministrazione(), x.getNomeArea(), x.getFasciaAnagrafica(), x.getSessoFemminile()));
    }



}
