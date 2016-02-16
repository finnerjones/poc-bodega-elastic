package com.finnerjones.es.bodega;

import org.elasticsearch.action.search.SearchResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by finner on 16/02/2016.
 */
public class EsBodegaTransportClientTest {

    private EsBodegaTransportClient transportClient;

    @Before
    public void init() {
        transportClient = new EsBodegaTransportClient();
    }


    @Test
    public void startupAndShutdown() {
        Assert.assertNotNull(transportClient.startup());
        Assert.assertTrue(transportClient.isRunning());
        Assert.assertTrue(transportClient.shutdown());
        Assert.assertFalse(transportClient.isRunning());
    }

    @Test
    public void matchAllQuery(){
        transportClient.startup();
        SearchResponse resp = transportClient.matchAllQuery("us", "gb");
        Assert.assertNotNull(resp);
        Assert.assertEquals(7,resp.getHits().getTotalHits());
        Assert.assertEquals("us",resp.getHits().getAt(0).getIndex());
        transportClient.shutdown();
    }

}
