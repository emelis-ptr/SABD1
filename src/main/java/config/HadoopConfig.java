package config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import utils.LogFile;

import java.io.IOException;

public class HadoopConfig {

    private HadoopConfig(){}

    /**
     * @return :
     */
    public static FileSystem configHDFS() {
        Configuration configuration = getConfigHDFS();

        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(configuration);
        } catch (IOException e) {
            LogFile.errorLog("Error in hdfs configuration");
        }

        return fileSystem;
    }

    /**
     * @return :
     */
    public static Configuration getConfigHDFS() {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://hdfs-namenode:9000");
        return configuration;
    }

    /**
     * @return :
     */
    public static String getPathHDFS() {
        Configuration configuration = getConfigHDFS();
        return configuration.get("fs.defaultFS");
    }

}
