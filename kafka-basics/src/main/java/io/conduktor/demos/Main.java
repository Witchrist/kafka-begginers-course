package io.conduktor.demos;

import io.conduktor.demos.kafka.consumer.ConsumerDemoCooperative;
import io.conduktor.demos.kafka.consumer.IConsumer;

public class Main {
    public static void main(String[] args) {

//        IProducer producer = new ProducerDemoKeys();
//        producer.sendMessage();

        IConsumer consumer = new ConsumerDemoCooperative();
        consumer.receiveMessage();
    }
}