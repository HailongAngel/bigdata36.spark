package day02;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @program: hello.spark
 * @description: JAVA的词频统计
 * 本地模式
 * @author: Hailong
 * @create: 2018-09-17 20:58
 **/
public class JavaWordCountLocal {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JavaWordCountLocal").setMaster("local[2]");

        //创建SparkContext,是Spark应用程序的入口
        JavaSparkContext jsc = new JavaSparkContext(conf);

        //指定以后从那里读取数据
        JavaRDD<String> lines = jsc.textFile("D:\\data\\in\\index\\b.txt");

        //分词压平
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        //将单词和1组合到一起
        JavaPairRDD<String,Integer> wordAndOne = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word,1);
            }
        });


        //分组聚合
        JavaPairRDD<String,Integer> reduce = wordAndOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });

        //将key和value的顺序颠倒（颠倒目的为了排序）
        JavaPairRDD<Integer,String> swaped = reduce.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tp) throws Exception {
                return tp.swap();
            }
        });
        //排序
        JavaPairRDD<Integer,String> sorted = swaped.sortByKey(false);

        //将key和和value顺序颠倒（颠倒目的为了排序）
        JavaPairRDD<String, Integer> result = sorted.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tp) throws Exception {
                return tp.swap();
            }
        });

        //触发一个计算，并且输出到屏幕
        List<Tuple2<String,Integer>> finalResult = result.collect();
        //输出到某个路径
        for (Tuple2<String,Integer> r:finalResult
             ) {
            System.out.println(r._1+" "+r._2);

        }
        //释放资源
        jsc.stop();
    }
}
