package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("hello");

        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //Set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int j=0; j<2; j++){
            for(int i=0; i<10; i++){

                String topic = "demo_java";
                String key = "id_" + i;
                String value = "Hello world - keys " + i;

                //Create a producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>( topic, key, value);

                //Send Data
                producer.send(producerRecord,new Callback(){
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e){
                        //Executes every tim e a record successfully sent or an exception is thrown
                        if(e==null){
                            //The record was successfully sent
                            log.info("Key: " + key  + " | Partition: "+metadata.partition());
                        }
                        else{
                            log.error("Error while producing",e);
                        }
                    }
                });
            }
        }

        //Tell the producer to send all data and block until done -- synchronous
        producer.flush();

        //Flush and close the producer -- close also call flush
        producer.close();
    }
}
