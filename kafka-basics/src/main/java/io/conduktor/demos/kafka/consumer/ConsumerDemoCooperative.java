package io.conduktor.demos.kafka.consumer;

import io.conduktor.demos.builder.ConsumerBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;

import static io.conduktor.demos.constants.KafkaConstants.TOPIC;

public class ConsumerDemoCooperative implements IConsumer{

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

    @Override
    public void receiveMessage() {

        String topic = TOPIC;

        //create a consumer
        KafkaConsumer<String, String> consumer = ConsumerBuilder.build();

        //get a reference to the main Thread
        final Thread mainThread = Thread.currentThread();

        //adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown, let's exit by calling consumer.wakeup()");

            //consumer.wakeup() throw a wakeup exception in the next time the consumer poll data (consumer.poll())
            consumer.wakeup();

            //join the main thread to allow the execution of the rest of the code in the main thread
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));

        try {
            //subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));

            //poll for data
            while (true) {
                log.info("Polling data");

            /*the Duration indicates the pauses that our consumer will have when there's no data retrieve from kafka
                in this example, when we don't receive any data from kafka, we'll wait 1 second to request again
            */
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + "     |   Value: " + record.value());
                    log.info("Partition: " + record.partition() + "     |   Offset: " + record.offset());
                }
            }
        } catch(WakeupException e){
            log.info("Consumer is starting to shutdown");
        } catch(Exception e){
            log.error("Unexpected Exception in the consumer", e);
        } finally {
            consumer.close();
            log.info("The consumer is gracefully shutdown");
        }
    }
}
