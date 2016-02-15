package com.finnerjones.es.bodega;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

/**
 * Created by finner on 26/01/2016.
 */
public class EsBodegaNodeClient {

    private Client nodeClient;
    private boolean running = false;

    public EsBodegaNodeClient() {
    }
    
    public Client startup() {
        if (nodeClient == null && !running) {
            Node node = NodeBuilder.nodeBuilder()
                    .settings(Settings.settingsBuilder()
                    .put("path.home", "C:\\development\\servers\\elasticsearch-2.1.1")
                    .put("http.enabled", false))
                    .client(true)
                    .node();
            nodeClient = node.client();
            running = true;
        }
        return nodeClient;
    }

    public boolean shutdown() {
        if (nodeClient != null && running) {
            try {
                nodeClient.close();
                running = false;
            } catch (Exception ex) {
                ex.printStackTrace();
                return false;
            }
        }
        return true;
    }

    public Settings settings() {
        if (nodeClient != null) {
            return nodeClient.settings();
        }
        return null;
    }


    public ActionFuture<SearchResponse> searchAll() {
        SearchRequest req = new SearchRequest();
        ActionFuture<SearchResponse> resp = nodeClient.search(req);
        return resp;
    }


    public SearchResponse matchAllCluster() {
        SearchResponse resp = nodeClient.prepareSearch().execute().actionGet();
        return resp;
    }

    public SearchRequestBuilder searchRequestBuilder() {
        return nodeClient.prepareSearch();
    }

    public boolean isRunning() {
        return running;
    }

}
