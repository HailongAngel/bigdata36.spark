package day02;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @program: HelloSparkNew
 * @description: Lambda版的WC
 *
 * @author: Hailong
 * @create: 2018-09-18 09:05
 **/
public class JavaLambdaWC {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("day02.JavaLambdaWC");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        //以后从来数据
        JavaRDD<String> lines = jsc.textFile(args[0]);

        //切分压平
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        //将每个单词记一次数
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(w -> new Tuple2<>(w, 1));

        //分组聚合
        JavaPairRDD<String, Integer> reduce = wordAndOne.reduceByKey((x, y) -> x + y);

        //因为必须使用key排序，所以需要交换一下key、value的位置
        reduce.mapToPair(tp -> tp.swap());
        reduce.sortByKey();
        reduce.mapToPair(tp -> tp.swap());

        reduce.saveAsTextFile(args[1]);

        //释放资源
        jsc.stop();

    }
}
