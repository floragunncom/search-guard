/*
 * Copyright 2021 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.floragunn.searchguard.test.helper.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.Client;
import org.junit.Assert;

import com.floragunn.searchguard.test.TestSgConfig;

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
