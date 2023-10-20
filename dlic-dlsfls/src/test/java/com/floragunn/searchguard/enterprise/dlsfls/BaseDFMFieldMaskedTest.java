/*
 * Copyright 2023 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */

package com.floragunn.searchguard.enterprise.dlsfls;

import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.xcontent.XContentType;

import java.util.function.Consumer;

public abstract class BaseDFMFieldMaskedTest {

    protected static final TestSgConfig.Role SG_DFM_ANON_2 = new TestSgConfig.Role("sg_dfm_anon_2")
            .clusterPermissions("*")
            .indexPermissions("*")
            .on("deals-outdated-*");

    protected static final TestSgConfig.Role SG_DFM_ANON_1 = new TestSgConfig.Role("sg_dfm_anon_1")
            .clusterPermissions("*")
            .indexPermissions("*")
            .fls("~customer.name", "~ip_source")
            .maskedFields("ip_dest")
            .on("deals-*");

    protected static final TestSgConfig.Role SG_DFM_NV_RESTRICTED = new TestSgConfig.Role("sg_dfm_nv_restricted")
            .clusterPermissions("indices:monitor/*", "SGS_CLUSTER_COMPOSITE_OPS")
            .indexPermissions("indices:monitor/*", "indices:admin/*", "indices:data/read/search*",
                    "indices:data/read/msearch*", "indices:data/read/suggest*"
            )
            .dls("{ \"bool\": { \"must_not\": { \"term\": { \"field1\": 1  }}}}")
            .fls("~field3", "~field4")
            .maskedFields("field2")
            .on("index1-*");

    protected static final TestSgConfig.Role SG_DFM_NV_NOT_RESTRICTED_ALL_INDICES = new TestSgConfig.Role("sg_dfm_nv_not_restricted_all_indices")
            .clusterPermissions("indices:monitor/*", "SGS_CLUSTER_COMPOSITE_OPS")
            .indexPermissions("indices:monitor/*", "indices:admin/*", "indices:data/read/search*",
                    "indices:data/read/msearch*", "indices:data/read/suggest*"
            )
            .on("index1-*");

    protected static final TestSgConfig.Role SG_DFM_NV_NOT_RESTRICTED_ONE_INDEX = new TestSgConfig.Role("sg_dfm_nv_not_restricted_one_index")
            .clusterPermissions("indices:monitor/*", "SGS_CLUSTER_COMPOSITE_OPS")
            .indexPermissions("indices:monitor/*", "indices:admin/*", "indices:data/read/search*",
                    "indices:data/read/msearch*", "indices:data/read/suggest*"
            )
            .on("index1-4")
            .indexPermissions("indices:monitor/*", "indices:admin/*", "indices:data/read/search*",
                    "indices:data/read/msearch*", "indices:data/read/suggest*"
            )
            .on("index1-1");

    protected static final TestSgConfig.User ADMIN = new TestSgConfig.User("admin").roles(TestSgConfig.Role.ALL_ACCESS);
    protected static final TestSgConfig.User DFM_USER = new TestSgConfig.User("dfm_user").roles(SG_DFM_ANON_2, SG_DFM_ANON_1);
    protected static final TestSgConfig.User DFM_RESTRICTED_ROLE = new TestSgConfig.User("dfm_restricted_role").roles(SG_DFM_NV_RESTRICTED);
    protected static final TestSgConfig.User DFM_RESTRICTED_AND_UNRESTRICTED_ALL_INDICES_ROLE = new TestSgConfig
            .User("dfm_restricted_and_unrestricted_all_indices_role").roles(SG_DFM_NV_NOT_RESTRICTED_ALL_INDICES, SG_DFM_NV_RESTRICTED);
    protected static final TestSgConfig.User DFM_RESTRICTED_AND_UNRESTRICTED_TWO_INDICES_ROLE = new TestSgConfig
            .User("dfm_restricted_and_unrestricted_two_indices_role").roles(SG_DFM_NV_RESTRICTED, SG_DFM_NV_NOT_RESTRICTED_ONE_INDEX);

    protected static final TestSgConfig.DlsFls DLS_FLS_CONFIG = new TestSgConfig.DlsFls().useImpl("flx");

    protected static LocalCluster clusterWithDfmEmptyOverridesAll(boolean dfmEmptyOverridesAll) {
        return new LocalCluster.Builder()
                .sslEnabled()
                .enterpriseModulesEnabled()
                .nodeSettings("searchguard.dfm_empty_overrides_all", dfmEmptyOverridesAll)
                .users(ADMIN, DFM_USER, DFM_RESTRICTED_ROLE, DFM_RESTRICTED_AND_UNRESTRICTED_ALL_INDICES_ROLE, DFM_RESTRICTED_AND_UNRESTRICTED_TWO_INDICES_ROLE)
                .dlsFls(DLS_FLS_CONFIG)
                .build();
    }

    protected static final Consumer<LocalCluster> TEST_DATA_PRODUCER = localCluster -> {
        try (Client client = localCluster.getInternalNodeClient()) {
            client.index(new IndexRequest("deals-0").id("0").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source("{\"customer\": {\"name\":\"cust1\"}, \"ip_source\": \"100.100.1.1\",\"ip_dest\": \"123.123.1.1\",\"amount\": 10}", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("deals-1").id("1").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source("{\"customer\": {\"name\":\"cust2\"}, \"ip_source\": \"100.100.2.2\",\"ip_dest\": \"123.123.2.2\",\"amount\": 20}", XContentType.JSON)).actionGet();
            client.index(new IndexRequest("deals-2").id("2").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source("{\"customer\": {\"name\":\"cust3\"}, \"ip_source\": \"100.100.2.3\",\"ip_dest\": \"123.123.3.2\",\"amount\": 30}", XContentType.JSON)).actionGet();

            client.index(new IndexRequest("deals-outdated-1").id("1").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source("{\"customer\": {\"name\":\"cust4\"}, \"ip_source\": \"100.100.32.1\",\"ip_dest\": \"123.123.4.2\",\"amount\": 100}", XContentType.JSON)).actionGet();

            client.index(new IndexRequest("deals-outdated-2").id("2").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source("{\"customer\": {\"name\":\"cust5\"}, \"ip_source\": \"100.100.3.2\",\"ip_dest\": \"123.123.5.2\",\"amount\": 200}", XContentType.JSON)).actionGet();

            client.index(new IndexRequest("deals-outdated-3").id("3").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source("{\"customer\": {\"name\":\"cust6\"}, \"ip_source\": \"100.100.3.3\",\"ip_dest\": \"123.123.6.2\",\"amount\": 300}", XContentType.JSON)).actionGet();

            client.index(new IndexRequest("index1-1").id("0").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source("{\"field1\": 1, \"field2\": \"value-2-1\", \"field3\": \"value-3-1\", \"field4\": \"value-4-1\" }", XContentType.JSON)).actionGet();

            client.index(new IndexRequest("index1-2").id("0").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source("{\"field1\": 2, \"field2\": \"value-2-2\", \"field3\": \"value-3-2\", \"field4\": \"value-4-2\" }", XContentType.JSON)).actionGet();

            client.index(new IndexRequest("index1-3").id("0").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source("{\"field1\": 3, \"field2\": \"value-2-3\", \"field3\": \"value-3-3\", \"field4\": \"value-4-3\" }", XContentType.JSON)).actionGet();

            client.index(new IndexRequest("index1-4").id("0").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source("{\"field1\": 4, \"field2\": \"value-2-4\", \"field3\": \"value-3-4\", \"field4\": \"value-4-4\" }", XContentType.JSON)).actionGet();
        }
    };
}
