package queries;

import config.SparkConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import utils.LogFile;

public class Main {

    public static void main(String[] args) {
        //LogFile.setupLogger();  //Log

        SparkConf conf = SparkConfig.sparkConfig();
        JavaSparkContext sc = new JavaSparkContext(conf);

        long start = System.nanoTime();

        QueryOne.queryOne(sc);

        long endQueryOne = System.nanoTime();
        long startQueryTwo = System.nanoTime();

        QueryTwo.queryTwo(sc);

        long end = System.nanoTime();

        LogFile.infoLog("Query One processed in : " + (int) (endQueryOne - start) / 1000000 + " ms");
        LogFile.infoLog("Query Two processed in : " + (int) (end - startQueryTwo) / 1000000 + " ms");
        LogFile.infoLog("Processed in : " + (int) (end - start) / 1000000 + " ms");

        sc.close();
    }
}
