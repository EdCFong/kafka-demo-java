package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("hello");

        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //Set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size","400");
        //properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        //Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //Sending a lot of batches
        for(int j=0; j<10; j++){
            for(int i=0; i<30; i++){
                //Create a producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "Hello world - callback" + i);

                //Send Data
                producer.send(producerRecord,new Callback(){
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e){
                        //Executes every tim e a record successfully sent or an exception is thrown
                        if(e==null){
                            //The record was successfully sent
                            log.info("Received new metadata\n"+
                                    "Topic:"+metadata.topic()+"\n"+
                                    "Partition:"+metadata.partition()+"\n"+
                                    "Offset:"+metadata.offset()+"\n"+
                                    "Timestamp:"+metadata.timestamp());
                        }
                        else{
                            log.error("Error while producing",e);
                        }
                    }
                });
            }

            try{
                Thread.sleep(500);
            }catch(InterruptedException e){
                e.printStackTrace();
            }
        }

        //Tell the producer to send all data and block until done -- synchronous
        producer.flush();

        //Flush and close the producer -- close also call flush
        producer.close();
    }
}
