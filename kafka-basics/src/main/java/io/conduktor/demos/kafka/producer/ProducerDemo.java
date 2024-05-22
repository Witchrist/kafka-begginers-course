package io.conduktor.demos.kafka.producer;


import io.conduktor.demos.builder.ProducerBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.conduktor.demos.constants.KafkaConstants.TOPIC;

public class ProducerDemo implements IProducer{

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    @Override
    public void sendMessage(){
        log.info("I am a Kafka Producer!");

        KafkaProducer<String, String> producer = ProducerBuilder.build();

        //create a Producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC, "Hello World");

        //send data
        producer.send(producerRecord);

        //tell the producer to send all data and block until done -- synchronous
        producer.flush();

        //flush and close the producer
        producer.close();
    }
}
