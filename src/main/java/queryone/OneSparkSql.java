package queryone;

import entity.PuntiSomministrazioneTipologia;
import entity.SomministrazioniVacciniSummaryLatest;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple3;
import utils.Constants;
import utils.Parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static utils.Constants.PATH_PST;

public class OneSparkSql {

    /**
     *
     * @param values:
     * @param pstValues:
     */
    public static void process(JavaRDD<Tuple3<String, String, Integer>> values, JavaRDD<Tuple3<String, String, String>> pstValues) {

        //The entry point to programming Spark with the Dataset and DataFrame API.
        SparkSession spark = SparkSession
                .builder()
                .appName("Queries").master("local")
                //.config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> schemaPST = createSchemaPST(spark, pstValues);
        Dataset<Row> schemaSVSL = createSchemaSVSL(spark, values);

        // Register the DataFrame as a SQL temporary view
        schemaSVSL.createOrReplaceTempView("schemaSVSL");
        schemaPST.createOrReplaceTempView("schemaPST");

        Dataset<Row> countDS = spark.sql("select distinct area, count(denominazione_struttura) as count from schemaPST group by area");
        countDS.createOrReplaceTempView("schemaCountPST");

        Dataset<Row> schemaFinal = spark.sql("SELECT a.area, a.data_somministrazione as mese,  (sum(a.totale)/b.count) as media FROM schemaSVSL a, schemaCountPST b where a.area = b.area GROUP BY a.area, a.data_somministrazione, b.count");

        schemaFinal.show(50);

        spark.close();
    }


    /**
     *
     * @param spark:
     * @param values:
     * @return :
     */
    private static Dataset<Row> createSchemaSVSL(SparkSession spark, JavaRDD<Tuple3<String, String, Integer>> values) {

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("data_somministrazione", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("area", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("totale", DataTypes.IntegerType, true));
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD to Rows
        JavaRDD<Row> rowRDD = values.map( val
                -> RowFactory.create(val._1(), val._2(), val._3()));

        // Apply the schema to the RDD
        return spark.createDataFrame(rowRDD, schema);

    }

    /**
     *
     * @param spark:
     * @param values:
     * @return :
     */
    private static Dataset<Row> createSchemaPST(SparkSession spark, JavaRDD<Tuple3<String, String, String>> values) {

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("area", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("denominazione_struttura", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("nome_regione", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD to Rows
        JavaRDD<Row> rowRDD = values.map( val
                -> RowFactory.create(val._1(), val._2(), val._3()));

        // Apply the schema to the RDD
        return spark.createDataFrame(rowRDD, schema);

    }


    public static JavaRDD<Tuple3<String, String, Integer>> queryOne2(JavaSparkContext sc) {
        JavaRDD<String> energyFile = sc.textFile(Constants.PATH_PST);

        JavaRDD<SomministrazioniVacciniSummaryLatest> outlets =
                energyFile.map(
                        Parser::parseCsvSVSL)
                        .filter(Objects::nonNull);

        return outlets.map(x -> new Tuple3<>
                (x.getMonth(), x.getArea(), x.getTotale()));
    }

    public static JavaRDD<Tuple3<String, String, String>> pstQuery(JavaSparkContext sc) {
        JavaRDD<String> energyFile = sc.textFile(PATH_PST);

        JavaRDD<PuntiSomministrazioneTipologia> outlets =
                energyFile.map(
                        Parser::parseCSVPST)
                        .filter(p -> p != null);

        return outlets.map(x -> new Tuple3<>
                (x.getArea(), x.getDenominazioneStruttura(), x.getNomeRegione()));
    }
}
