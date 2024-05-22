package io.conduktor.demos.builder;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class ConsumerBuilder {

    public static KafkaConsumer<String, String> build(){

        String groupId = "my-java-application";

        //create Producer Properties
        Properties properties = new Properties();

        //connect to localhost
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

        //set consumer properties
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        /*possible values in the AUTO_OFFSET_RESET_CONFIG
            none -> if we don't have an existing consumer group, than we fail (the consumer group must be set before starting the application)
            earliest -> earliest reads the data from the beggining of the topic (its the --from-beggining basically)
            latest -> reads only the new messages
        */
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        return consumer;
    }
}
