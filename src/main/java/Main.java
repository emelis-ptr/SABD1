import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import queryone.QueryOne;
import queryone.OneSparkSql;
import querytwo.QueryTwo;
import scala.Tuple3;
import utils.SparkConfig;

import java.text.ParseException;

public class Main {

    public static void main(String[] args) throws ParseException {
        SparkConf conf = SparkConfig.sparkConfig();
        JavaSparkContext sc = SparkConfig.initJavaSparkContext(conf);
        SparkSession sparkSession = SparkConfig.sparkSession();

        long start = System.nanoTime();

        QueryOne.queryOne(sparkSession, sc);
        QueryTwo.queryTwo(sc, sparkSession);

        long end = System.nanoTime();
        System.out.println("Processed in : " + (int) (end - start) / 1000000 + " ms");

        sparkSession.close();
        SparkConfig.sparkStop(sc);
    }

    /**
     * @param sc:
     */
    private static void sparkSQL(JavaSparkContext sc) {
        JavaRDD<Tuple3<String, String, Integer>> value = OneSparkSql.queryOne2(sc);
        JavaRDD<Tuple3<String, String, String>> pstValues = OneSparkSql.pstQuery(sc);

        OneSparkSql.process(value, pstValues);
    }




}
