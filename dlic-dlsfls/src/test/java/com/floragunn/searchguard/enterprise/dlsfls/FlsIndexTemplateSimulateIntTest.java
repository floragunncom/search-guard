/*
 * Copyright 2026 by floragunn GmbH - All rights reserved
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

import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

/**
 * To validate an index template, Elasticsearch creates a temporary index on the thread of the request, i.e., with the
 * user present, and asks the FLS field filter for it. Such an index is not part of the cluster state.
 */
public class FlsIndexTemplateSimulateIntTest {

    static final DocNode TEMPLATE = DocNode.of("index_patterns", DocNode.array("sim_*"), "template",
            DocNode.of("mappings.properties.dept.type", "keyword", "aliases.sim_alias.filter.term.dept", "a"));

    static final TestSgConfig.User USER = new TestSgConfig.User("user")
            .roles(new Role("all_but_secret").indexPermissions("*").fls("~secret").on("*").clusterPermissions("*"));

    static final TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db"));

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().enterpriseModulesEnabled().authc(AUTHC).users(USER)
            .build();

    @BeforeClass
    public static void setupTestData() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            assertThat(client.putJson("/_index_template/sim_template", TEMPLATE), isOk());
            assertThat(client.putJson("/sim_existing", DocNode.EMPTY), isOk());
            assertThat(client.put("/sim_existing/_alias/sim_existing_alias"), isOk());
        }
    }

    @Test
    public void simulateTemplate() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER)) {
            assertThat(client.postJson("/_index_template/_simulate", TEMPLATE.with("priority", 10)), isOk());
            assertThat(client.post("/_index_template/_simulate/sim_template"), isOk());
        }
    }

    @Test
    public void simulateIndex() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER)) {
            assertThat(client.post("/_index_template/_simulate_index/sim_new"), isOk());
            assertThat(client.post("/_index_template/_simulate_index/sim_existing"), isOk());
            assertThat(client.post("/_index_template/_simulate_index/sim_existing_alias"), isOk());
        }
    }
}
