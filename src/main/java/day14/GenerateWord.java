package day14;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.stringtemplate.v4.ST;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

/**
 * @program: HelloSparkNew
 * @description: 产生随机数据源
 * @author: Hailong
 * @create: 2018-10-14 11:27
 **/
public class GenerateWord {
    public static void main(String[] args) throws InterruptedException {
        //封装配置参数
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");//kafka的brokers列表
        //key和value的序列化方式，因为需要网络传输所以需要序列化
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //创建一个生产者的客户端实例
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        while(true){
            Thread.sleep(500);

            //生成一个随机的key
            String key = UUID.randomUUID().toString();

            //随机生成一个单词
            int base = 97;
            int acciCode = new Random().nextInt(26) + base;

            char word = (char) acciCode;
            //System.out.println("word = "+ word);

            ProducerRecord<String, String> record = new ProducerRecord<>("wordCount", key, String.valueOf(word));

            kafkaProducer.send(record);

            System.out.println("record = "+ record);
        }
    }
}
