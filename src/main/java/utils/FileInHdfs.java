package utils;

import config.HadoopConfig;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

import static utils.Constants.*;

public class FileInHdfs {

    private FileInHdfs() {
    }

    private static final String[] pathFiles = {PATH_PST, PATH_SVSL, PATH_SVL};

    /**
     * Metodo che copia i dati di input su HDFS
     */
    public static void copyFileToHDFS() {
        FileSystem fileSystem = HadoopConfig.configHDFS();

        for (String file : pathFiles) {
            Path hdfsPath = new Path(DIR_HDFS + file);

            try {
                if (!fileSystem.exists(hdfsPath)) {
                    fileSystem.create(hdfsPath, true);
                    fileSystem.copyFromLocalFile(new Path(file), hdfsPath);
                }
            } catch (IOException e) {
                LogFile.errorLog("Error in coping file in hdfs");
            }
        }
    }

    public static void deleteDir() {
        FileSystem fileSystem = HadoopConfig.configHDFS();
        try {
            fileSystem.delete(new Path("/sabd/data"), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
