package utils;

import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.io.FileWriter;

public class WriteFile {

    /**
     * Metodo che scrive in un file csv i risultati ottenuti dalla prima query
     *
     * @param res:
     */
    public static void writeQueryOne(JavaPairRDD<String, Tuple2<String, Float>> res) {
        String outname = "result/resultQueryOne.csv";

        try (FileWriter fileWriter = new FileWriter(outname)) {

            fileWriter.append("Mese,Regione,Media");
            fileWriter.append("\n");

            for (Tuple2<String, Tuple2<String, Float>> ou : res.collect()) {

                fileWriter.append(ou._1);
                fileWriter.append(",");
                fileWriter.append(ou._2._1);
                fileWriter.append(",");
                fileWriter.append(String.valueOf(ou._2._2));
                fileWriter.append("\n");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
