import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import queryone.QueryOne;
import queryone.OneSparkSql;
import scala.Tuple2;
import scala.Tuple3;
import utils.SparkConfig;
import utils.WriteFile;


public class Main {

    public static void main(String[] args) {
        long start = System.nanoTime();

        SparkConf conf = SparkConfig.sparkConfig();
        JavaSparkContext sc = SparkConfig.initJavaSparkContext(conf);

        JavaPairRDD<String, Tuple2<String, Float>>  resultQueryOne = QueryOne.queryOne(sc);

        long end = System.nanoTime();

        WriteFile.writeQueryOne(resultQueryOne);
        System.out.println("Processed in : " + (int) (end - start) / 1000000 + " ms");
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
