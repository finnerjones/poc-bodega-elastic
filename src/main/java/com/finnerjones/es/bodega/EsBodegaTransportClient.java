package com.finnerjones.es.bodega;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by finner on 16/02/2016.
 */
public class EsBodegaTransportClient {

    private Client client;
    private boolean running = false;

    public Client startup() {
        try {
            if (client == null && !running) {
                client = TransportClient.builder().build()
                        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
                running = true;
            }
        } catch (UnknownHostException uhe) {
            uhe.printStackTrace();
        }
        return client;
    }

    public boolean shutdown() {
        if (running) {
            client.close();
            running = false;
            return true;
        }
        return false;
    }


    public SearchResponse matchAllQuery(String... indices) {
        SearchResponse resp = client.prepareSearch(indices)
                .setQuery(QueryBuilders.matchAllQuery()).setExplain(false)
                .setSize(50)
                .execute()
                .actionGet();
        return resp;
    }


    public boolean isRunning() {
        return running;
    }
}
