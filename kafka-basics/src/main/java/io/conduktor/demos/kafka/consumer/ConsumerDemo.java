package io.conduktor.demos.kafka.consumer;

import io.conduktor.demos.builder.ConsumerBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;

import static io.conduktor.demos.constants.KafkaConstants.TOPIC;

public class ConsumerDemo implements IConsumer{

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    @Override
    public void receiveMessage() {

        String topic = TOPIC;

        //create a consumer
        KafkaConsumer<String, String> consumer = ConsumerBuilder.build();

        //subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        //poll for data
        while (true){
            log.info("Polling data");

            /*the Duration indicates the pauses that our consumer will have when there's no data retrieve from kafka
                in this example, when we don't receive any data from kafka, we'll wait 1 second to request again
            */
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for(ConsumerRecord<String, String> record : records){
                log.info("Key: " + record.key() + "     |   Value: " + record.value());
                log.info("Partition: " + record.partition() + "     |   Offset: " + record.offset());
            }
        }
    }
}
