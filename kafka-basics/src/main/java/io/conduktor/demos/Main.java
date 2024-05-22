package io.conduktor.demos;

import io.conduktor.demos.kafka.IProducer;
import io.conduktor.demos.kafka.ProducerDemoWithCallback;

public class Main {
    public static void main(String[] args) {

        IProducer producer = new ProducerDemoWithCallback();

        producer.sendMessage();
    }
}