package io.conduktor.demos;

import io.conduktor.demos.utils.ClientUtil;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class OpenSearchMain {
    public static void main(String[] args) throws IOException {
        Logger log = LoggerFactory.getLogger(OpenSearchMain.class.getSimpleName());

        //create an OpenSearch client
        RestHighLevelClient openSearchClient = ClientUtil.createOpenSearchClient();
        ClientUtil.createIndexRequest(openSearchClient);
        //create the index on OpenSearch if ir doesn't exist already

    }
}