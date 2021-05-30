import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import queries.QueryOne;
import queries.QueryTwo;
import utils.FileInHdfs;
import utils.LogFile;
import config.SparkConfig;
import utils.RenameFile;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.ParseException;

public class Main {

    public static void main(String[] args) throws ParseException, IOException, URISyntaxException {
        LogFile.setupLogger();

        SparkConf conf = SparkConfig.sparkConfig();
        JavaSparkContext sc = SparkConfig.initJavaSparkContext(conf);
        SparkSession sparkSession = SparkConfig.sparkSession();

        FileInHdfs.copyFileToHDFS();

        long start = System.nanoTime();

       // QueryOne.queryOne(sparkSession, sc);
        QueryTwo.queryTwo(sc, sparkSession);

        long end = System.nanoTime();
        System.out.println("Processed in : " + (int) (end - start) / 1000000 + " ms");

        sparkSession.close();
        SparkConfig.sparkStop(sc);

        RenameFile.renameFile();
    }





}
