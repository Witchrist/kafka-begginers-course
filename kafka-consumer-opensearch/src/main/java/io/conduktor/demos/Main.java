package io.conduktor.demos;

import io.conduktor.demos.utils.ClientUtil;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(Main.class.getSimpleName());

        //create an OpenSearch client
        RestHighLevelClient openSearchClient = ClientUtil.createOpenSearchClient();

        //create the index on OpenSearch if ir doesn't exist already

    }
}