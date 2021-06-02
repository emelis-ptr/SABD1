package utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static utils.Constants.*;

public class RenameFile {

    private RenameFile() {
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
