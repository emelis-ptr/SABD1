import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple3;

import static utils.Constants.PATH_PST;

public class QueryOne {

    public static void queryOne(JavaSparkContext sc){
        JavaRDD<String> energyFile = sc.textFile(PATH_PST);


    }
}
