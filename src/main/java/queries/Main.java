package queries;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import utils.FileInHdfs;
import utils.LogFile;
import config.SparkConfig;
import utils.RenameFile;

public class Main {

    public static void main(String[] args) {
        //LogFile.setupLogger();  //Log

        SparkSession sparkSession = SparkConfig.sparkSession();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        FileInHdfs.copyFileToHDFS();

        long start = System.nanoTime();

        QueryOne.queryOne(sparkSession, sc);
        QueryTwo.queryTwo(sc, sparkSession);

        long end = System.nanoTime();
        LogFile.infoLog("Processed in : " + (int) (end - start) / 1000000 + " ms");

        sc.close();
        sparkSession.close();

        RenameFile.renameFile();
    }
}
