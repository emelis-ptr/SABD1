package toRemove;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.io.FileWriter;
import java.util.List;

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

    public static void writeQueryTwo(JavaPairRDD<Tuple2<String, String>, Iterable<Tuple2<Double, String>>> res) {
        String outname = "result/resultQueryTwo.csv";

        try (FileWriter fileWriter = new FileWriter(outname)) {

            fileWriter.append("Anno-mese,Fascia-Anagrafica,Area,Prediction");
            fileWriter.append("\n");

            //data, fascia-anagrafica -- area, prediction
            for (Tuple2<Tuple2<String, String>, Iterable<Tuple2<Double, String>>> ou : res.collect()) {

                List<Tuple2<Double, String>> list = IteratorUtils.toList(ou._2.iterator());
                for(Tuple2<Double, String> entry : list){
                    fileWriter.append(ou._1._1);
                    fileWriter.append(",");
                    fileWriter.append(ou._1._2);
                    fileWriter.append(",");
                    fileWriter.append(entry._2);
                    fileWriter.append(",");
                    fileWriter.append(String.valueOf(entry._1));
                    fileWriter.append("\n");
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
