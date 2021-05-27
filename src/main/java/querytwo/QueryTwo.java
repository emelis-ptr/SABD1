package querytwo;

import dataset.DatasetS;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.*;
import scala.Tuple2;
import scala.Tuple3;
import utils.ComparatorTuple;
import utils.Data;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.*;

import static java.util.Calendar.MONTH;
import static java.util.Calendar.YEAR;
import static utils.Constants.*;

public class QueryTwo {

    private QueryTwo() {
    }

    /**
     * Query two
     *
     * @param sc:
     * @param spark:
     * @throws ParseException:
     */
    public static void queryTwo(JavaSparkContext sc, SparkSession spark) throws ParseException {

        JavaRDD<String> dataFile = sc.textFile(PATH_SVL);
        JavaPairRDD<Tuple3<String, String, String>, Tuple2<Long, Integer>> initialData = initialData(dataFile);

        //conta per ogni key il numero di giorni in cui sono stati effettutati i vaccini
        Map<Tuple3<String, String, String>, Long> countNumDaysMap = initialData.countByKey();
        JavaPairRDD<Tuple3<String, String, String>, Long> countNumDays = mapToJavaPairRDD(sc, countNumDaysMap);

        Broadcast<List<Tuple3<String, String, String>>> filterKeyMinor2 = filterNumDayMinor2(sc, countNumDays);
        //filter
        initialData = initialData.filter(x -> !filterKeyMinor2.value().contains(x._1));
        //groupByKey
        JavaPairRDD<Tuple3<String, String, String>, Iterable<Tuple2<Long, Integer>>> groupInitData = initialData
                .groupByKey()
                .sortByKey(new ComparatorTuple.Tuple3ComparatorString(), true);

        //linear regression
        List<Tuple2<Tuple3<String, String, String>, Double>> prediction = linearRegression(spark, groupInitData);

        //data, area, fascia-anagrafica -- prediction
        JavaPairRDD<Tuple3<String, String, String>, Double> predictionPair = sc.parallelizePairs(prediction);

        //fascia-anagrafica, data -- area, prediction
        JavaPairRDD<Tuple2<String, String>, Tuple2<Double, String>> predictionTop = predictionPair
                .mapToPair(
                        row -> new Tuple2<>(new Tuple2<>(row._1._1(), row._1._3()), new Tuple2<>(row._2, row._1._2()))
                )
                .groupByKey()
                .flatMapToPair(
                        row -> {
                            List<Tuple2<Double, String>> tuple2 = IteratorUtils.toList(row._2.iterator());
                            tuple2.sort(Collections.reverseOrder(new ComparatorTuple.Tuple2ComparatorStringDouble()));

                            List<Tuple2<Tuple2<String, String>, Tuple2<Double, String>>> list = new ArrayList<>();

                            for (Tuple2<Double, String> tuple : tuple2) {
                                list.add(new Tuple2<>(new Tuple2<>(row._1._1, row._1._2), tuple));
                            }

                            return list.stream().limit(5).iterator();
                        }
                )
                //.groupByKey()
                .sortByKey(new ComparatorTuple.Tuple2ComparatorString(), true);

        Dataset<Row> createSchema = DatasetS.createSchemaQuery2(spark, predictionTop);
        createSchema.show(100);
        createSchema.coalesce(1).write().mode(SaveMode.Overwrite).option("header",true).format(CSV_FORMAT).save(RES_DIR_QUERY2);

    }

    /**
     * Metodo che attravero la regressione lineare predice
     * il numero di vaccinazione per il primo giorno del mese successivo
     *
     * @param sparkSession:
     * @param groupInitData:
     * @return :
     * @throws ParseException:
     */
    private static List<Tuple2<Tuple3<String, String, String>, Double>> linearRegression(SparkSession sparkSession, JavaPairRDD<Tuple3<String, String, String>, Iterable<Tuple2<Long, Integer>>> groupInitData) throws ParseException {
        List<Tuple2<Tuple3<String, String, String>, Double>> prediction = new ArrayList<>();

        for (Tuple2<Tuple3<String, String, String>, Iterable<Tuple2<Long, Integer>>> entry : groupInitData.collect()) {

            List<Tuple2<Long, Integer>> numVaccToDays = IteratorUtils.toList(entry._2.iterator());
            Encoder<Tuple2<Long, Integer>> encoders = Encoders.tuple(Encoders.LONG(), Encoders.INT());

            Dataset<Row> dataset = sparkSession.createDataset(numVaccToDays, encoders).toDF("data", "numVaccini");

            VectorAssembler vectorAssembler = new VectorAssembler()
                    .setInputCols(new String[]{"data"})
                    .setOutputCol(FEATURES);

            LinearRegression lr = new LinearRegression()
                    .setMaxIter(10)
                    .setRegParam(0.3)
                    .setElasticNetParam(0.8)
                    .setTol(0.1)
                    .setFeaturesCol(FEATURES)
                    .setLabelCol("numVaccini");

            // Fit the model.
            LinearRegressionModel lrModel = lr.fit(vectorAssembler.transform(dataset));

            Date nextMonth = Data.getNextMonth(entry._1._1()).getTime();

            double predict = lrModel.predict(Vectors.dense(nextMonth.getTime()));
            prediction.add(new Tuple2<>(entry._1, predict));

            // Print the coefficients and intercept for linear regression.
            // System.out.println("Coefficients: "+ lrModel.coefficients() + " Intercept: " + lrModel.intercept());
        }
        return prediction;
    }

    /**
     * Metodo che restituisce una coppia:
     * Data(anno-mese), area, fascia_anagrafica -- data , num_vacc_femminile
     *
     * @param dataFile:
     * @return :
     */
    private static JavaPairRDD<Tuple3<String, String, String>, Tuple2<Long, Integer>> initialData(JavaRDD<String> dataFile) {
        //Data(anno-mese), area, fascia_anagrafica -- data , num_vacc_femminile
        return dataFile
                .mapToPair(
                        s -> {
                            String[] svslSplit = s.split(SPLIT_COMMA);
                            //default, ISO_LOCAL_DATE
                            LocalDate localDate = LocalDate.parse(svslSplit[0]);

                            //Data, area, fascia-anagrafica -- num_vac_femminili
                            return new Tuple2<>(new Tuple3<>(localDate, svslSplit[21], svslSplit[3]), Integer.valueOf(svslSplit[5]));
                        })
                .filter(x -> !x._1._1().isBefore(LocalDate.parse(IGNORE_DATE_BEFORE))) //filtra ignorando tutte le date prima 2021-02-01
                .reduceByKey(Integer::sum)  //somma il numero di vacc-femminile per la stessa chiave (dato che ci sono più tipologie)
                .mapToPair(
                        s -> {
                            String yearMonth = "";
                            long date = 0;
                            try {
                                Date date1 = new SimpleDateFormat(DATE_WITH_DASHES).parse(String.valueOf(s._1._1()));
                                Calendar cal = Calendar.getInstance();
                                cal.setTime(date1);
                                yearMonth = cal.get(YEAR) + "-" + (cal.get(MONTH) + 1);

                                DateFormat dateFormat = new SimpleDateFormat(DATE_NO_DASHES);
                                String strDate = dateFormat.format(date1);
                                date = Long.parseLong(strDate);
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                            //Data(anno-mese), area, fascia_anagrafica -- data , num_vacc_femminile
                            return new Tuple2<>(new Tuple3<>(yearMonth, s._1._2(), s._1._3()), new Tuple2<>(date, s._2));
                        }
                );
    }

    /**
     * Metodo che filtra considerando una chiave il cui
     * numero dei giorni di vaccinazione sia minore di 2
     *
     * @param sc:
     * @param res:
     * @return :
     */
    private static Broadcast<List<Tuple3<String, String, String>>> filterNumDayMinor2(JavaSparkContext sc, JavaPairRDD<Tuple3<String, String, String>, Long> res) {
        return sc
                .broadcast(res.filter(row -> row._2 < 2)
                        .keys()
                        .collect());
    }

    /**
     * Metodo che converte la mappa in JavaPairRDD
     *
     * @param sc:
     * @param countNumDays:
     * @return :
     */
    private static JavaPairRDD<Tuple3<String, String, String>, Long> mapToJavaPairRDD(JavaSparkContext sc, Map<Tuple3<String, String, String>, Long> countNumDays) {
        List<Tuple2<Tuple3<String, String, String>, Long>> list = new ArrayList<>();
        for (Map.Entry<Tuple3<String, String, String>, Long> entry : countNumDays.entrySet()) {
            list.add(new Tuple2<>(entry.getKey(), entry.getValue()));
        }

        //Per trasformare in JavaRDD
        return sc.parallelizePairs(list);
    }
}
