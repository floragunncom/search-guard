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

import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import com.floragunn.searchguard.client.RestHighLevelClient;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

import java.util.Map;

public class BulkTests {
    
    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled()
            .user("bulk_test_user", "secret", new Role("bulk_test_user_role").clusterPermissions("*").indexPermissions("*").on("test")).build();

    @Test
    public void testBulk() throws Exception {

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("bulk_test_user", "secret")) {

            BulkRequest.Builder br = new BulkRequest.Builder();
            br.refresh(Refresh.True);
            br.operations(new BulkOperation.Builder().index(new IndexOperation.Builder<Map>().document(Map.of("a", "b")).index("test").id("1").build()).build(),
                          new BulkOperation.Builder().index(new IndexOperation.Builder<Map>().document(Map.of("a", "b")).index("myindex").id("1").build()).build());
            BulkResponse res = client.getJavaClient().bulk(br.build());
            Assert.assertTrue(res.errors());
            Assert.assertEquals(2, res.items().size());
            Assert.assertTrue(res.items().get(0).error() == null); //ok because we have all perms for test
            Assert.assertFalse(res.items().get(1).error() == null); //fails because no perms for myindex
        }
    }
}
