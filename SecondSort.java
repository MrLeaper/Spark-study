import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.codehaus.janino.Java;
import scala.Tuple2;

public class SecondSort {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkStudy");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("test");
        JavaPairRDD<SecondarySortKey, String> pairs = lines.mapToPair(new PairFunction<String, SecondarySortKey, String>() {
            public Tuple2<SecondarySortKey, String> call(String s) throws Exception {
                String[] linesplit = s.split(" ");
                SecondarySortKey Key = new SecondarySortKey(
                        Integer.valueOf(linesplit[0]),Integer.valueOf(linesplit[1])
                );
                return new Tuple2<SecondarySortKey, String>(Key,s);
            }
        });
        JavaPairRDD<SecondarySortKey, String> sortpairs = pairs.sortByKey();
        JavaRDD<String> sortlins = sortpairs.map(new Function<Tuple2<SecondarySortKey, String>, String>() {
            public String call(Tuple2<SecondarySortKey, String> secondarySortKeyStringTuple2) throws Exception {
                return secondarySortKeyStringTuple2._2;
            }
        });
        sortlins.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        sc.close();

    }
}
