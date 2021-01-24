/*
 * Copyright 2015-2020 floragunn GmbH
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

package com.floragunn.searchguard;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.cluster.TestSgConfig.Role;

public class BulkTests {

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled()
            .user("bulk_test_user", "secret", new Role("bulk_test_user_role").clusterPermissions("*").indexPermissions("*").on("test")).build();

    @Test
    public void testBulk() throws Exception {

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("bulk_test_user", "secret")) {

            BulkRequest br = new BulkRequest();
            br.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
            br.add(new IndexRequest("test").id("1").source("a", "b"));
            br.add(new IndexRequest("myindex").id("1").source("a", "b"));

            BulkResponse res = client.bulk(br, RequestOptions.DEFAULT);
            Assert.assertTrue(res.hasFailures());
            Assert.assertEquals(200, res.status().getStatus());
            Assert.assertFalse(res.getItems()[0].isFailed());
            Assert.assertTrue(res.getItems()[1].isFailed());
        }
    }
}
