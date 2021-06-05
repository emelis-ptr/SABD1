package queries;

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
import utils.Utils;

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

    /**
     ** Query two **
     * Per le donne, per ogni fascia anagrafica e per ogni mese solare,
     * determinare le prime 5 aree per le quali è previsto il maggior numero di vaccinazioni il primo giorno del mese successivo.
     * Per determinare la classifica mensile e prevedere il numero di vaccinazioni,
     * considerare la retta di regressione che approssima l’andamento delle vaccinazioni giornaliere.
     * Per la risoluzione della query, considerare le sole categorie per cui nel mese solare in esame vengono registrati
     * almeno due giorni di campagna vaccinale.
     * Viene inoltre richiesto di calcolare la classifica per ogni mese e categoria a partire dai dati raccolti dall’1 Febbraio 2021.
     *
     * @param sc:
     */
    public static void queryTwo(JavaSparkContext sc){
        //Data(anno-mese), area, fascia_anagrafica -- data , num_vacc_femminile
        JavaPairRDD<Tuple3<String, String, String>, Tuple2<Date, Integer>> initialData = initialData(sc);

        //conta per ogni key il numero di giorni in cui sono stati effettutati i vaccini
        //Data(anno-mese), area, fascia_anagrafica -- numDaysToMonth
        Map<Tuple3<String, String, String>, Long> countNumDaysMap = initialData.countByKey();
        JavaPairRDD<Tuple3<String, String, String>, Long> countNumDays = mapToJavaPairRDD(sc, countNumDaysMap);

        //Data(anno-mese), area, fascia_anagrafica
        Broadcast<List<Tuple3<String, String, String>>> filterKeyMinor2 = filterNumDayMinor2(sc, countNumDays);
        //filter
        initialData = initialData.filter(x -> !filterKeyMinor2.value().contains(x._1));

        // Data(anno-mese), area, fascia_anagrafica -- data , num_vacc_femminile
        JavaPairRDD<Tuple3<String, String, String>, Iterable<Tuple2<Date, Integer>>> groupInitData = initialData
                .groupByKey()
                .sortByKey(new ComparatorTuple.Tuple3ComparatorString(), true);

        //Data, fascia-anagrafica, simpleRegression -- area
        JavaPairRDD<Tuple3<Date, String, Double>,  String> simpleRegression = simpleRegression(groupInitData);

        Dataset<Row> createSchema = Schema.createSchemaQuery3(sc, simpleRegression);
        createSchema.coalesce(1).write().mode(SaveMode.Overwrite).option(HEADER, true).format(CSV_FORMAT).save("hdfs://hdfs-namenode:9000/results/queryTwo/");

        createSchema.show(50);
   }

    /**
     * Metodo che predice il numero di vaccini del primo giorno del mese successivo
     * con simple regression
     *
     * @param lines:
     * @return :
     */
    public static JavaPairRDD<Tuple3<Date, String, Double>,  String> simpleRegression(JavaPairRDD<Tuple3<String, String, String>, Iterable<Tuple2<Date, Integer>>> lines) {

        //Data, fascia-anagrafica, simpleRegression -- area
        return lines
                //anno-mese, fascia-anagrafica -- prediction, area
                .mapToPair( line -> { // Data(anno-mese), area, fascia_anagrafica -- data , num_vacc_femminile
                    SimpleRegression regression = new SimpleRegression();
                    for (Tuple2<Date, Integer> en : line._2) {
                        regression.addData((double) en._1.getTime(), (double) en._2);
                    }

                    Date nextMonth = Utils.getNextMonth(line._1._1()).getTime();
                    double predictedVaccinations = regression.predict((double) (nextMonth).getTime());

                    return new Tuple2<>(new Tuple2<>(nextMonth, line._1._3()), new Tuple2<>(predictedVaccinations, line._1._2()));
                })
                .groupByKey()
                .sortByKey(new ComparatorTuple.Tuple2ComparatorDateString())
                .flatMapToPair(
                        line -> {
                            List<Tuple2<Double, String>> list = IteratorUtils.toList(line._2.iterator());
                            list.sort(Collections.reverseOrder(Comparator.comparing(o -> o._1)));

                            List<Tuple2<Tuple3<Date, String, Double>, String>> topFive = new ArrayList<>();

                            for(Tuple2<Double, String> tuple2: list){
                                topFive.add(new Tuple2<>(new Tuple3<>(line._1._1, line._1._2, tuple2._1), tuple2._2));
                            }
                            return topFive.stream().limit(5).iterator();
                        });
    }

    /**
     * Metodo che restituisce una coppia:
     * Data(anno-mese), area, fascia_anagrafica -- data , num_vacc_femminile
     *
     * @return :
     */
    private static JavaPairRDD<Tuple3<String, String, String>, Tuple2<Date, Integer>> initialData(JavaSparkContext sc) {
        JavaRDD<Tuple4<String, String, String, Integer>> svl = initialDataSVL(sc);

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
    public static JavaRDD<Tuple4<String, String, String, Integer>> initialDataSVL(JavaSparkContext sc) {
        JavaRDD<String> svlFile = sc
                .textFile("hdfs://hdfs-namenode:9000/data/somministrazioni-vaccini-latest.csv")
                .sortBy(arg0 -> arg0.split(SPLIT_COMMA)[0], true, 10);

        //Data(anno-mese), area, fascia_anagrafica -- data , num_vacc_femminile
        JavaRDD<SomministrazioniVacciniLatest> somministrazioniVacciniLatestJavaRDD = SomministrazioniVacciniLatest.getInstance(svlFile);

        return somministrazioniVacciniLatestJavaRDD.map(x -> new Tuple4<>(x.getDataSomministrazione(), x.getNomeArea(), x.getFasciaAnagrafica(), x.getSessoFemminile()));
    }



}
