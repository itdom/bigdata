package cn.gxlx.etl.kafka.example2;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

public class kafkaProducer extends Thread {

    private String topic;

    public kafkaProducer(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public void run() {
        Producer<Integer, String> producer = createProducer();
        int i = 0;
        while (true) {
            producer.send(new KeyedMessage<Integer, String>(topic, "message: " + i++));
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private Producer<Integer, String> createProducer() {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", "slave01:2181,slave04:2181,slave03:2181");//声明zk  
        properties.put("serializer.class", StringEncoder.class.getName());
        properties.put("metadata.broker.list", "slave06:9093,slave05:9093");// 声明kafka broker  
        return new Producer<Integer, String>(new ProducerConfig(properties));
    }

    public static void main(String[] args) {
        new kafkaProducer("test4").start();// 使用kafka集群中创建好的主题 test   

    }

}