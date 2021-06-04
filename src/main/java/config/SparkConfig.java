package config;

import org.apache.spark.SparkConf;

public class SparkConfig {

    private SparkConfig() {
    }

    /**
     * @return :
     */
    public static SparkConf sparkConfig() {
        return new SparkConf()
                .setMaster("spark://spark-master:7077")
                .setAppName("Sabd project");
    }
}
