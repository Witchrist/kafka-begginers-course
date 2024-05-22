package io.conduktor.demos.kafka.producer;


import io.conduktor.demos.builder.ProducerBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static io.conduktor.demos.constants.KafkaConstants.TOPIC;

public class ProducerDemoKeys implements IProducer{

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    @Override
    public void sendMessage(){
        log.info("I am a Kafka Producer with Callback!");

        KafkaProducer<String, String> producer = ProducerBuilder.build();

        for(int j = 0; j<2; j++) {

            System.out.println("---------------------------------------------------------");

            for (int i = 0; i < 5; i++) {
                String topic = TOPIC;
                String key = "id_" + i;
                String value = "Hello World " + i;

                this.sendMessage(producer, createMessage(topic, key, value), key);
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        //tell the producer to send all data and block until done -- synchronous
        producer.flush();

        //flush and close the producer
        producer.close();
    }

    private ProducerRecord createMessage(String topic, String key, String value){

        //create a Producer record
        return new ProducerRecord<>(topic, key, value);
    }

    private void sendMessage(KafkaProducer<String, String> producer, ProducerRecord producerRecord, String key){

        //send data
        producer.send(producerRecord, (metadata, e) -> {

            //executes everytime when a record successfully sent or when an exception is thrown
            if (Objects.isNull(e)) {

                //the record was successfully sent
                log.info("Key: " + key + "  |   Partition: " + metadata.partition());
            } else {
                log.error("Error while producing", e);
            }
        });
    }
}
