package com.finnerjones.es.bodega;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
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
        Assert.assertTrue(nodeClient.isRunning());
    }

    @Test
    public void nodeShutdown() {
        Assert.assertTrue(nodeClient.shutdown());
        Assert.assertFalse(nodeClient.isRunning());
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
        try {
            nodeClient.startup();
            resp = nodeClient.searchAll().get();
            nodeClient.shutdown();
        } catch (InterruptedException iex) {
            iex.printStackTrace();
        } catch (ExecutionException eex) {
            eex.printStackTrace();
        }
        Assert.assertNotNull(resp);
        Assert.assertTrue(resp.getHits().totalHits() > 0);
        Assert.assertEquals(7, resp.getHits().totalHits());
    }

    @Test
    public void matchAllOnClusterWithDefaults() {
        nodeClient.startup();
        SearchResponse resp = nodeClient.matchAllCluster();
        nodeClient.shutdown();
        Assert.assertNotNull(resp);
    }


    @Test
    public void findJohnsTweets() {
        nodeClient.startup();
        SearchResponse resp = nodeClient.searchRequestBuilder()
                .setIndices("us")
                .setTypes("tweet")
                .setQuery(QueryBuilders.termQuery("name", "john"))
                .setFrom(0)
                .setSize(100)
                .execute()
                .actionGet();
        nodeClient.shutdown();
        Assert.assertEquals(1, resp.getHits().getTotalHits());
        for (SearchHit hit: resp.getHits()) {
            Assert.assertTrue(((String)resp.getHits().getAt(0).getSource().get("name")).toLowerCase().contains("john"));
        }
    }
}
