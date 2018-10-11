package day12;

/**
 * @program: HelloSparkNew
 * @description: kafka生产端api的开发
 * @author: Hailong
 * @create: 2018-10-10 23:11
 **/
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
public class ProducerApi {
    public static void main(String[] args) throws Exception {
        //封装配置参数
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");//kafka的brokers列表
        //key和value的序列化方式，因为需要网络传输所以需要序列化
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        /**
         * 发送数据的时候是否需要应答
         * 取值范围：
         *  [all, -1, 0, 1]
         *  0：leader不做任何应答
         *  1：leader会给producer做出应答
         *  all、-1：fllower->leader -> producer
         * 默认值：
         *  1
         */
        //props.setProperty("acks", "1");

        /**
         * 自定义分区
         * 默认值：org.apache.kafka.clients.producer.internals.DefaultPartitioner
         */
        //props.setProperty("partitioner.class", "org.apache.kafka.clients.producer.internals.DefaultPartitioner");

        //创建一个生产者的客户端实例
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        int count = 0;
        while (count < 1000) {
            int partitionNum = count % 3;

            //封装一条消息
            ProducerRecord record = new ProducerRecord("testTopic", partitionNum, "", count + "");
            //发送一条消息
            kafkaProducer.send(record);

            count++;
            Thread.sleep(1 * 1000);
        }
        //释放
        kafkaProducer.close();
        System.out.println("send End...");
    }
}
