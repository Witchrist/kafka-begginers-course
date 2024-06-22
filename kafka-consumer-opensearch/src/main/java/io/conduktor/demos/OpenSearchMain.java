package io.conduktor.demos;

import io.conduktor.demos.kafka.builder.ConsumerBuilder;
import io.conduktor.demos.opensearch.OpenSearchDataSender;
import io.conduktor.demos.utils.ClientUtil;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class OpenSearchMain {
    public static void main(String[] args) throws IOException {
        Logger log = LoggerFactory.getLogger(OpenSearchMain.class.getSimpleName());

        //create an OpenSearch client
        RestHighLevelClient openSearchClient = ClientUtil.createOpenSearchClient();

        //create a kafka consumer
        KafkaConsumer<String, String> consumer = ConsumerBuilder.createKafkaConsumer();

        //create the index on OpenSearch if ir doesn't exist already
        ClientUtil.createIndexRequest(openSearchClient);

        //Sending data to OpenSearch
        OpenSearchDataSender openSearchDataSender = new OpenSearchDataSender();
        openSearchDataSender.sendData(openSearchClient, consumer);



    }
}