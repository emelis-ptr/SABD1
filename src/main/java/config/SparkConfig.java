package config;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkConfig {

    private SparkConfig(){}

    /**
     * @return :
     */
    public static SparkConf sparkConfig() {
        return new SparkConf()
                .setMaster("spark://0.0.0.0:7077")
                .setAppName("project");
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
