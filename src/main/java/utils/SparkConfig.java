package utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkConfig {

    private SparkConfig(){}

    /**
     * @return :
     */
    public static SparkConf sparkConfig() {
        return new SparkConf()
                .setMaster("local")
                .setAppName("project");
    }

    /**
     * @param config:
     * @return :
     */
    public static JavaSparkContext initJavaSparkContext(SparkConf config) {
        JavaSparkContext sc = new JavaSparkContext(config);
        sc.setLogLevel("Error");

        return sc;
    }

    /**
     * @param sc:
     */
    public static void sparkStop(JavaSparkContext sc) {
        sc.stop();
    }

    /**
     *
     * @return :
     */
    public static SparkSession sparkSession(){
       return SparkSession
                .builder()
                .appName("Project")
                .master("local")
                .getOrCreate();
    }
}
