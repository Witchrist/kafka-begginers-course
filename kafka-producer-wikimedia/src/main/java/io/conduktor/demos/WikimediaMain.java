package io.conduktor.demos;

import io.conduktor.demos.kafka.wikimedia.WikimediaChangesProducer;

public class WikimediaMain {
    public static void main(String[] args) {
        WikimediaChangesProducer wikimedia = new WikimediaChangesProducer();

        try {
            wikimedia.start();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}