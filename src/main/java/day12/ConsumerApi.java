package day12;

/**
 * @program: HelloSparkNew
 * @description: 消费端API的开发
 * @author: Hailong
 * @create: 2018-10-10 23:14
 **/
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
public class ConsumerApi {
    public static void main(String[] args) {

        HashMap<String, Object> config  = new HashMap<String, Object>();
        config.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        config.put("key.deserializer", StringDeserializer.class.getName());
        config.put("value.deserializer", StringDeserializer.class.getName());
        config.put("group.id", "g000001");

        /**
         * 从哪个位置开始获取数据
         * 取值范围：
         *  [latest, earliest, none]
         * 默认值：
         *  latest
         */
        config.put("auto.offset.reset", "earliest");
        /**
         * 是否要自动递交偏移量（offset）这条数据在某个分区所在位置的编号
         */
        config.put("enable.auto.commit", false);

        //创建一个消费者客户端实例
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(config);
        //订阅主题（告诉客户端从哪个主题获取数据）
        kafkaConsumer.subscribe(Arrays.asList("testTopic"));

        while (true) {
            //拉去数据， 会从kafka所有分区下拉取数据
            ConsumerRecords<String, String> records = kafkaConsumer.poll(2000);
            Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
            while (iterator.hasNext()) {
                ConsumerRecord<String, String> record = iterator.next();
                System.out.println("record = " + record);
            }
        }

        //释放连接
        //kafkaConsumer.close();
    }
}
