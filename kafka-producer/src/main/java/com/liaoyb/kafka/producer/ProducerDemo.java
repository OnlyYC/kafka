package com.liaoyb.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;

public class ProducerDemo {
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);
    public static void main(String[] args) throws InterruptedException {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "192.168.33.11:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);

        //发送消息
        String msg = "random_value:"+ UUID.randomUUID().toString();
        logger.info("#######################send message:"+msg);
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("test",msg);
        try {
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    logger.info("***************完成了******************");
                }
            });
        }catch (Exception e){
            e.printStackTrace();
        }

        Thread.sleep(2000);

    }
}
