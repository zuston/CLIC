package fdu.daslab.executable.udf;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class WordCountSpark {
    public static void f(JavaSparkContext sparkContext,String path) {
        JavaRDD<String> rdd1 = sparkContext.textFile(path);

        JavaRDD<String> rdd2 = rdd1.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                List<String> list = new ArrayList<>();
                String[] arr = s.split(" ");
                for(String ss : arr) {
                    list.add(ss);
                }
                return list.iterator();
            }
        });

        JavaPairRDD<String,Integer> rdd3 = rdd2.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String,Integer>(s,1);
            }
        });

        JavaPairRDD<String,Integer> rdd4 = rdd3.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        JavaPairRDD<String,Integer> rdd5 = rdd4.sortByKey();

        for(Object item : rdd5.collect()) {
            System.out.println(item);
        }

    }

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("WordCountDemon");
        sparkConf.setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        f(sparkContext,"/Users/zhuxingpo/Downloads/test.txt");
    }

}

