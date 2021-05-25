package queryone;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import utils.Comparator;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Month;
import java.time.format.TextStyle;
import java.util.*;

import static java.util.Calendar.MONTH;
import static utils.Constants.PATH_PST;
import static utils.Constants.PATH_SVSL;

public class QueryOne {

    /**
     * Query one
     *
     * @param sc:
     * @return :
     */
    public static JavaPairRDD<String, Tuple2<String, Float>> queryOne(JavaSparkContext sc) {
        JavaPairRDD<String, Long> pstPair = getJavaPairPST(sc);
        JavaPairRDD<String, Tuple2<String, Integer>> svslPair = getPairSLSV(sc);

        //Join
        JavaPairRDD<String, Tuple2<Long, Tuple2<String, Integer>>> joined = pstPair.join(svslPair);

        return joined.mapToPair(
                s -> {
                    float avg = (float) s._2._2._2 / s._2._1;
                    String month = Month.of(Integer.parseInt(s._2._2._1)).getDisplayName(TextStyle.FULL, Locale.ITALIAN);
                    return new Tuple2<>(month.toUpperCase(), new Tuple2<>(s._1, avg));
                });
    }

    /**
     * Metodo che prende dal file punti-somministrazione-tipologia il nome della regione e
     * conta quanti centri ci sono per ogni data regione
     *
     * @param sc:
     * @return :
     */
    private static JavaPairRDD<String, Long> getJavaPairPST(JavaSparkContext sc) {
        JavaRDD<String> pst = sc.textFile(PATH_PST);

        //Nome regione, count centri
        Map<String, Long> count = pst.mapToPair(
                s -> {
                    String[] svslSplit = s.split(",");
                    return new Tuple2<>(svslSplit[6], svslSplit[1]);
                }).distinct().countByKey();

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
    private static JavaPairRDD<String, Tuple2<String, Integer>> getPairSLSV(JavaSparkContext sc) {
        JavaRDD<String> svsl = sc.textFile(PATH_SVSL)
                .sortBy((Function<String, String>) arg0 -> arg0.split(",")[0], true, 10);

        //Nome regione, Data, Totale e ordina in base al numero del mese
        // e tramite reducebyKey somma i valori associati alla stessa chiave
        JavaPairRDD<Tuple2<String, String>, Integer> svslPairs =
                svsl.mapToPair(
                        s -> {
                            String[] svslSplit = s.split(",");
                            String month = "";
                            try {
                                Date date1 = new SimpleDateFormat("yyyy-MM-dd").parse(svslSplit[0]);
                                Calendar cal = Calendar.getInstance();
                                cal.setTime(date1);
                                month = String.valueOf(cal.get(MONTH) + 1);

                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                            return new Tuple2<>(new Tuple2<>(svslSplit[20], month), Integer.valueOf(svslSplit[2]));
                        }).filter(x -> !x._1._2.equals("12".toUpperCase())).reduceByKey(Integer::sum)
                        .sortByKey(new Comparator.TupleComparator(), true);


        //Nome regione, Data, Totale
        return svslPairs.mapToPair(
                s -> new Tuple2<>(s._1._1, new Tuple2<>(s._1._2, s._2)));
    }


}
