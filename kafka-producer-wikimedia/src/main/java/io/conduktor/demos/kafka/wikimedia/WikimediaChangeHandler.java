package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {
    private KafkaProducer<String, String> producer;
    private String topic;

    private final Logger log = LoggerFactory.getLogger(this.getClass().getSimpleName());

    public WikimediaChangeHandler(KafkaProducer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen(){
        //Nothing here
    }

    @Override
    public void onClosed(){
        producer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent){

        log.info(messageEvent.getData());

        //asynchronous
        producer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String comment){
        //Nothing here
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Error in stream reading", throwable);
    }
}
