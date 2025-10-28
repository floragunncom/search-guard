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

import com.floragunn.codova.documents.DocNode;

import com.floragunn.searchguard.test.GenericRestClient;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsFieldPointedByJsonPath;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.docNodeSizeEqualTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;


public class BulkTests {
    
    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled()
            .user("bulk_test_user", "secret", new Role("bulk_test_user_role").clusterPermissions("*").indexPermissions("*").on("test")).build();

    @Test
    public void testBulk() throws Exception {

        try (GenericRestClient client = cluster.getRestClient("bulk_test_user", "secret")) {

            String bulkBody = """
                    {"index":{"_id":"1","_index":"test"}}
                    { "a" : "b" }
                    {"index":{"_id":"1","_index":"myindex"}}
                    { "a" : "b" }
                    """;

            GenericRestClient.HttpResponse res = client.postJson("/_bulk?refresh=true", bulkBody);

            assertThat(res, isOk());
            assertThat(res.getBodyAsDocNode(), containsValue("$.errors", true));
            assertThat(res.getBodyAsDocNode(), docNodeSizeEqualTo("$.items", 2));

            DocNode firstItem = res.getBodyAsDocNode().findSingleNodeByJsonPath("items[0].index");
            DocNode secondItem = res.getBodyAsDocNode().findSingleNodeByJsonPath("items[1].index");

            assertThat(firstItem, not(containsFieldPointedByJsonPath("$", "error")));
            assertThat(secondItem, containsFieldPointedByJsonPath("$", "error"));
            assertThat(secondItem, containsValue("$.error.type", "security_exception"));
        }
    }
}
