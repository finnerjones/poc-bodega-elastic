package com.finnerjones.es.bodega;

import static org.elasticsearch.node.NodeBuilder.*;

/**
 * Created by finner on 26/01/2016.
 */
public class EsBodegaNodeClient {

    public static void main(String[] args) {
        Node node = nodeBuilder().node();
        Client client = node.client();
    }
}
