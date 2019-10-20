import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import java.util.Arrays;
import java.util.List;
import java.util.Iterator;
import java.util.Map;

import java.util.ArrayList;

public class GroupTop {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("hellp");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("test");
        JavaPairRDD<String,Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] linesplit = s.split(" ");
                return new Tuple2<String, Integer>(linesplit[0],Integer.valueOf(linesplit[1]));
            }
        });
        JavaPairRDD<String, Iterable<Integer>> grouppairs = pairs.groupByKey();
        JavaPairRDD<String, Iterable<Integer>> top3score = grouppairs.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                Integer[] top3 = new Integer[3];
                String classname = stringIterableTuple2._1;
                Iterator<Integer> scores = stringIterableTuple2._2.iterator();
                while(scores.hasNext()){
                    Integer score = scores.next();
                    for (int i = 0; i < 3; i++) {
                        if(top3[i] == null){
                            top3[i] = score;
                            break;
                        }else if(score > top3[i]){
                            int tmp = top3[i];
                            top3[i] = score;
                            if(i < top3.length-1){
                                top3[i + 1] = tmp;
                            }
                        }
                    }
                }
                return new Tuple2<String, Iterable<Integer>>(classname,Arrays.asList(top3));
            }
        });
        top3score.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            public void call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                System.out.print(stringIterableTuple2._1 + ":");
                Iterator<Integer> iterator = stringIterableTuple2._2.iterator();
                while(iterator.hasNext()){
                    System.out.println(iterator.next());
                }

            }
        });
    }
}
