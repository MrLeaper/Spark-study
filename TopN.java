import javafx.scene.shape.HLineTo;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class TopN {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("hellp");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("test");
        JavaPairRDD<Integer, String> pairs = lines.mapToPair(new PairFunction<String, Integer, String>() {
            public Tuple2<Integer, String> call(String s) throws Exception {
                return new Tuple2<Integer, String>(Integer.valueOf(s), s);
            }
        });
        JavaPairRDD<Integer, String> sortpairs = pairs.sortByKey(false);
        JavaRDD<Integer> sortrdd = sortpairs.map(new Function<Tuple2<Integer, String>, Integer>() {
            public Integer call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return integerStringTuple2._1;
            }
        });
        List<Integer> list = sortrdd.take(3);
        for (Integer integer : list) {
            System.out.println(integer);
        }
    }
}
