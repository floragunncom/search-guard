package com.floragunn.searchguard.test.helper.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.Client;
import org.junit.Assert;

import java.util.function.Supplier;

class SearchGuardIndexInitializer {

    private static final Logger log = LogManager.getLogger(SearchGuardIndexInitializer.class);

    static void initSearchGuardIndex(Supplier<Client> getAdminCertClient, TestSgConfig testSgConfig) {
        log.info("Initializing Search Guard index");

        try (Client client = getAdminCertClient.get()) {
            testSgConfig.initIndex(client);

            Assert.assertTrue(client.get(new GetRequest("searchguard", "config")).actionGet().isExists());
            Assert.assertTrue(client.get(new GetRequest("searchguard", "internalusers")).actionGet().isExists());
            Assert.assertTrue(client.get(new GetRequest("searchguard", "roles")).actionGet().isExists());
            Assert.assertTrue(client.get(new GetRequest("searchguard", "rolesmapping")).actionGet().isExists());
            Assert.assertTrue(client.get(new GetRequest("searchguard", "actiongroups")).actionGet().isExists());
            Assert.assertFalse(client.get(new GetRequest("searchguard", "rolesmapping_xcvdnghtu165759i99465")).actionGet().isExists());
            Assert.assertTrue(client.get(new GetRequest("searchguard", "config")).actionGet().isExists());
        }
    }
}
