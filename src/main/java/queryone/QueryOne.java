package queryone;

import dataset.DatasetS;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import utils.ComparatorTuple;
import utils.Data;

import java.text.ParseException;
import java.time.Month;
import java.time.format.TextStyle;
import java.util.*;

import static java.util.Calendar.MONTH;
import static utils.Constants.*;

public class QueryOne {

    private QueryOne(){}

    /**
     * Query one
     *
     * @param sc:
     */
    public static void queryOne(SparkSession sparkSession, JavaSparkContext sc) {
        JavaPairRDD<String, Long> pstPair = getJavaPairPST(sc);
        JavaPairRDD<String, Tuple2<String, Integer>> svslPair = getPairSLSV(sc);

        //Join
        JavaPairRDD<String, Tuple2<Long, Tuple2<String, Integer>>> joined = pstPair.join(svslPair);

        JavaPairRDD<String, Tuple2<String, Float>> finalRes = joined.mapToPair(
                s -> {
                    float avg = (float) s._2._2._2 / s._2._1;
                    String month = Month.of(Integer.parseInt(s._2._2._1)).getDisplayName(TextStyle.FULL, Locale.ITALIAN);
                    return new Tuple2<>(month.toUpperCase(), new Tuple2<>(s._1, avg));
                });

        Dataset<Row> createSchema = DatasetS.createSchemaQuery1(sparkSession, finalRes);
        createSchema.show(100);
        createSchema.coalesce(1).write().mode(SaveMode.Overwrite).option("header",true).format(CSV_FORMAT).save(RES_DIR_QUERY1);
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
                .sortBy(arg0 -> arg0.split(SPLIT_COMMA)[0], true, 10);

        //Nome regione, Data, Totale e ordina in base al numero del mese
        // e tramite reducebyKey somma i valori associati alla stessa chiave
        JavaPairRDD<Tuple2<String, String>, Integer> svslPairs =
                svsl.mapToPair(
                        s -> {
                            String[] svslSplit = s.split(SPLIT_COMMA);
                            String month = "";
                            try {
                                Calendar cal = Data.getMonth(svslSplit[0]);
                                month = String.valueOf(cal.get(MONTH) + 1);

                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                            return new Tuple2<>(new Tuple2<>(svslSplit[20], month), Integer.valueOf(svslSplit[2]));
                        }).filter(x -> !x._1._2.equalsIgnoreCase(IGNORE_MONTH)).reduceByKey(Integer::sum)
                        .sortByKey(new ComparatorTuple.Tuple2ComparatorString(), true);

        //Nome regione, Data, Totale
        return svslPairs.mapToPair(
                s -> new Tuple2<>(s._1._1, new Tuple2<>(s._1._2, s._2))).sortByKey();
    }


}
