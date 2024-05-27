package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import io.conduktor.demos.kafka.builder.ProducerBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.net.URI;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {


    public void start() throws InterruptedException {
        KafkaProducer<String, String> producer = ProducerBuilder.build();

        String topic = "wikimedia.recentchange";
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();

        //start the producer in another thread
        eventSource.start();

        //produce for 10 minutes and block the program until then
        TimeUnit.MINUTES.sleep(10);
    }
}
