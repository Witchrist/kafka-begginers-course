package io.conduktor.demos.opensearch;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class OpenSearchDataSender {

    private final static Logger log = LoggerFactory.getLogger(OpenSearchDataSender.class.getSimpleName());

    public void sendData(RestHighLevelClient openSearchClient, KafkaConsumer<String, String> consumer){

        //subscribing the consumer to a topic
        consumer.subscribe(Collections.singleton("wikimedia.recentchange"));

        while(true){
            ConsumerRecords<String, String> records = consumer.poll(3000);

            int recordsCount = records.count();
            log.info("Received: "+ recordsCount +" record(s)");

            records.forEach(record -> {
                //Send the records into OpenSearch
                IndexRequest indexRequest = new IndexRequest("wikimedia")
                        .source(record.value(), XContentType.JSON);

                try {
                    IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

                    log.info(indexResponse.getId());
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
            });
        }
    }
}
