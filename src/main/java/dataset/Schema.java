package dataset;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple3;
import utils.Utils;

import java.util.*;

public class Schema {

    private Schema() {
    }

    /**
     * Metodo che crea un dataset
     *
     * @param sc:
     * @param values:
     * @return :
     */
    public static Dataset<Row> createSchemaFinalQuery1(JavaSparkContext sc, JavaPairRDD<String, Tuple2<String, Float>> values) {
        SQLContext sqlContext = new SQLContext(sc);
        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("mese", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("area", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("media", DataTypes.FloatType, true));
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD to Rows
        JavaRDD<Row> rowRDD = values.map(
                val ->
                {
                    Calendar cal = Utils.getMonthFromString(val._1);
                    String month = cal.getDisplayName(Calendar.MONTH, Calendar.LONG, Locale.ITALIAN);
                    String monthUpper = month.substring(0, 1).toUpperCase() + "" + month.substring(1);
                    return RowFactory.create(monthUpper, val._2._1, val._2._2);
                }
        );
        // Apply the schema to the RDD
        return sqlContext.createDataFrame(rowRDD, schema);
    }

    /**
     * Metodo che crea un dataset
     *
     * @param sc:
     * @param values:
     * @return :
     */
    public static Dataset<Row> createSchemaQuery3(JavaSparkContext sc, JavaPairRDD<Tuple3<Date, String, Double>, String> values) {
        SQLContext sqlContext = new SQLContext(sc);
        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("mese", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("fascia-anagrafica", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("area", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("prediction", DataTypes.IntegerType, true));
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD to Rows
        JavaRDD<Row> rowRDD = values.map(
                val -> {
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(val._1._1());

                    String month = cal.getDisplayName(Calendar.MONTH, Calendar.LONG, Locale.ITALIAN).substring(0, 1).toUpperCase(Locale.ROOT) + cal.getDisplayName(Calendar.MONTH, Calendar.LONG, Locale.ITALIAN).substring(1);
                    String firstDayMonth = cal.get(Calendar.DAY_OF_MONTH) + " " + month;

                    int prediction = val._1._3().intValue();
                    return RowFactory.create(firstDayMonth, val._1._2(), val._2, prediction);
                }
        );
        // Apply the schema to the RDD
        return sqlContext.createDataFrame(rowRDD, schema);
    }

}
