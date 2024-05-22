package io.conduktor.demos;

import io.conduktor.demos.kafka.IProducer;
import io.conduktor.demos.kafka.ProducerDemoKeys;

public class Main {
    public static void main(String[] args) {

        IProducer producer = new ProducerDemoKeys();

        producer.sendMessage();
    }
}