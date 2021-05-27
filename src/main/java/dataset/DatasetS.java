package dataset;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import utils.Data;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;

import static java.util.Calendar.MONTH;

public class DatasetS {

    private DatasetS(){}

    /**
     * Metodo che crea un dataset
     *
     * @param spark:
     * @param values:
     * @return :
     */
    public static Dataset<Row> createSchemaQuery1(SparkSession spark, JavaPairRDD<String, Tuple2<String, Float>> values) {

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("mese", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("area", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("media", DataTypes.FloatType, true));
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD to Rows
        JavaRDD<Row> rowRDD = values.map(
                val -> RowFactory.create(val._1, val._2._1, val._2._2)
        );
        // Apply the schema to the RDD
        return spark.createDataFrame(rowRDD, schema);
    }

    /**
     * Metodo che crea un dataset
     *
     * @param spark:
     * @param values:
     * @return :
     */
    public static Dataset<Row> createSchemaQuery2(SparkSession spark, JavaPairRDD<Tuple2<String, String>, Tuple2<Double, String>> values) {

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("mese", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("fascia-anagrafica", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("area", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("prediction", DataTypes.DoubleType, true));
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD to Rows
        JavaRDD<Row> rowRDD = values.map(
                val -> {
                    String nextMonth = Data.getNextMonth(val._1._1).get(Calendar.DAY_OF_MONTH) + " " + Data.getNextMonth(val._1._1).getDisplayName(MONTH, Calendar.LONG, Locale.ITALIAN).toUpperCase(Locale.ROOT);
                    return RowFactory.create(nextMonth, val._1._2, val._2._2, val._2._1);
                }
        );
        // Apply the schema to the RDD
        return spark.createDataFrame(rowRDD, schema);
    }

}
