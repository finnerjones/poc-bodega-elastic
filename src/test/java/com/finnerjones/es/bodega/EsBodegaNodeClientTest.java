package com.finnerjones.es.bodega;

import org.elasticsearch.action.search.SearchResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

/**
 * Created by finner on 15/02/2016.
 */
public class EsBodegaNodeClientTest {

    private EsBodegaNodeClient nodeClient;


    @Before
    public void init() {
        nodeClient = new EsBodegaNodeClient();
    }


    @Test
    public void nodeClientStartup() {
        Assert.assertNotNull(nodeClient.startup());
    }

    @Test
    public void nodeShutdown() {
        Assert.assertTrue(nodeClient.shutdown());
    }

    @Test
    public void checkConfig() {
        nodeClient.startup();
        Assert.assertEquals("false", nodeClient.settings().get("http.enabled"));
        Assert.assertEquals("true", nodeClient.settings().get("node.client"));
        nodeClient.shutdown();
    }


    @Test
    public void searchAllIndexes() {
        SearchResponse resp = null;
        nodeClient.startup();
        try {
            resp = nodeClient.searchAll().get();
        } catch (InterruptedException iex) {
            iex.printStackTrace();
        } catch (ExecutionException eex) {
            eex.printStackTrace();
        }
        Assert.assertNotNull(resp);
        Assert.assertTrue(resp.getHits().totalHits() > 0);
        nodeClient.shutdown();
    }

}
