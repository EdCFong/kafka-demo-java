package io.conduktor.demos.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
jnryk
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am a Kafka Consumer!");

        String groupId = "my-java-application";
        String topic = "demo_java";

        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // create consumer configs
        properties.setProperties("key.deserializer", StringDeserializer.class.getName());
        properties.setProperties("value.deserializer", StringDeserializer.class.getName());
        properties.SetProperties("group.id", groupId);
        properties.SetProperties("auto.offset.reset", "earliest");

        // create a consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        // subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        // poll for data
        while(true){
            log.info("Polling");

            ConsumerRecord<String,String> record = consumer.poll(Duration.ofMillis(1000));

            for(ConsumerRecord<String,String> record: records){
              log.info("Key: " + record.key() + ", Value: " + record.value());
              log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }
    }
}
