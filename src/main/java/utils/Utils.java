package utils;

import config.HadoopConfig;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import static utils.Constants.*;

public class Utils {

    private Utils() {
    }

    private static final String[] pathFiles = {PATH_PST, PATH_SVSL, PATH_SVL};

    /**
     * Metodo che copia i dati di input su HDFS
     */
    public static void copyFileToHDFS() {
        FileSystem fileSystem = HadoopConfig.configHDFS();

        for (String file : pathFiles) {
            Path hdfsPath = new Path(DIR_HDFS + file);
            File fileLocal = new File(file);

            try {
               // if (!fileSystem.exists(hdfsPath) || fileSystem.getFileStatus(hdfsPath).getModificationTime() < fileLocal.lastModified()) {
                    fileSystem.create(hdfsPath, true);
                    fileSystem.copyFromLocalFile(new Path(file), hdfsPath);
               // }
            } catch (IOException e) {
                LogFile.errorLog("Error in coping file in hdfs");
            }
        }
    }

    /**
     * Metodo che prende il primo giorno del mese successivo
     *
     * @param date:
     * @return :
     * @throws ParseException :
     */
    public static Calendar getNextMonth(String date) throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat(YEAR_MONTH_FORMAT);
        Date dateNextMonth = dateFormat.parse(date + "-01");

        Calendar cal = Calendar.getInstance();
        cal.setTime(dateNextMonth);
        cal.add(Calendar.MONTH, 1);
        return cal;
    }

    /**
     * Metodo che prende il primo giorno del mese successivo
     *
     * @param date:
     * @return :
     * @throws ParseException :
     */
    public static Calendar getMonthFromString(String date) throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat(YEAR_MONTH_FORMAT);
        Date dateNextMonth = dateFormat.parse(date + "-01");

        Calendar cal = Calendar.getInstance();
        cal.setTime(dateNextMonth);
        return cal;
    }

    /**
     *
     */
    public static void renameFile() {
        renameAndDeleteFiles();
    }

    /**
     * @return :
     */
    public static boolean renameAndDeleteFiles() {
        String[] namesDir = {RES_DIR_QUERY1, RES_DIR_QUERY2};
        boolean bool = false;

        for (int i = 0; i < namesDir.length; i++) {
            File[] listFiles = new File(namesDir[i]).listFiles();

            assert listFiles != null;
            for (File file : listFiles) {
                if (!file.getName().endsWith(CSV_FORMAT)) {
                    try {
                        Files.delete(Paths.get(namesDir[i] + "/" + file.getName()));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if (file.renameTo(new File(namesDir[i] + "/resultQuery" + (i + 1) + ".csv"))) {
                    //file renamed
                    bool = true;
                }
            }
        }
        return bool;
    }
}
