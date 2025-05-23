package com.floragunn.signals;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.junit.ClassRule;
import org.junit.Test;

public class LeakTest {
    private static final Logger log = LogManager.getLogger(LeakTest.class);

    public static final String INDEX_NAME = "my_leak_index";

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().singleNode().sslEnabled().resources("sg_config/no-tenants")
            .nodeSettings("signals.enabled", true, "signals.index_names.log", "signals_main_log", "searchguard.enterprise_modules_enabled", false)
            .enableModule(SignalsModule.class).waitForComponents("signals").embedded().build();

    /**
     * The test should fails from time to time due to leak prevention
     */
    @Test
    public void causeLeak() {
        Client client = cluster.getInternalNodeClient();
        IndexRequest indexRequest = new IndexRequest(INDEX_NAME).source(DocNode.of("foo", "bar")).id("one")
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client.index(indexRequest).actionGet();

        SearchResponse searchResponse = client.search(new SearchRequest(INDEX_NAME)).actionGet();
        // no searchResponse.decRef(), leak here
        log.info("Search response: {}", searchResponse);
    }
}
