/*
 * Copyright 2022 by floragunn GmbH - All rights reserved
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

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.test.RestMatchers;
import com.floragunn.searchguard.test.TestData;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.equalTo;

@RunWith(Parameterized.class)
public class DlsWriteIntTest {

    static final String INDEX_PATTERN = "dls_*";


    static final TestSgConfig.User ADMIN = new TestSgConfig.User("admin")
            .roles(new Role("all_access").indexPermissions("*").on("*").clusterPermissions("*"));
    static final TestSgConfig.User DLS_USER = new TestSgConfig.User("dls_user")
            .roles(new Role("role").indexPermissions("SGS_MANAGE", "SGS_CRUD").dls(DocNode.of("term.dept.value", "dept_d")).on(INDEX_PATTERN).clusterPermissions("*"));

    static final TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db"));
    static final TestSgConfig.DlsFls DLSFLS = new TestSgConfig.DlsFls().useImpl("flx").metrics("detailed");
    public static final String LOGSDB_INDEX_POSTFIX = "logsdb";

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().enterpriseModulesEnabled().authc(AUTHC).dlsFls(DLSFLS)
            .users(ADMIN, DLS_USER).resources("dlsfls").build();

    private final String indexNamePostfix;

    @BeforeClass
    public static void beforeClass() throws Exception {
        try(GenericRestClient client = cluster.getAdminCertRestClient()) {
            DocNode body = DocNode.of("index_patterns", ImmutableList.of("*" + LOGSDB_INDEX_POSTFIX), "template.settings", ImmutableMap.of("index.mode", "logsdb"));
            GenericRestClient.HttpResponse response = client.putJson("/_index_template/logsdb", body);
            MatcherAssert.assertThat(response, RestMatchers.isOk());
        }
    }

    public DlsWriteIntTest(String indexNamePostfix) {
        this.indexNamePostfix = indexNamePostfix;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Object[] parameters() {
        return new Object[] { "_mode_normal", "_mode_" + LOGSDB_INDEX_POSTFIX };
    }

    @Test
    public void newIndex_allowedRead() throws Exception {
        String index = "dls_new_index_allowed_read" + indexNamePostfix;
        String doc1uri = "/" + index + "/_doc/1";

        try (GenericRestClient client = cluster.getRestClient(DLS_USER)) {
            GenericRestClient.HttpResponse response = client.putJson(doc1uri + "?refresh=true", DocNode.of("payload", "foo", "dept", "dept_d", "@timestamp", "2022-01-01T00:00:00Z"));
            Assert.assertEquals(response.getBody(), 201, response.getStatusCode());

            response = client.get(doc1uri);
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get(doc1uri);
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());

            if(index.endsWith(LOGSDB_INDEX_POSTFIX)) {
                assertThat(TestData.getIndexMode(client, index), equalTo("logsdb"));
            } else {
                // null means default mode which is currently normal
                assertThat(TestData.getIndexMode(client, index), anyOf(equalTo("normal"), nullValue()));
            }
        }
    }

    @Test
    public void newIndex_disallowedRead() throws Exception {
        String index = "dls_new_index_disallowed_read" + indexNamePostfix;
        String doc1uri = "/" + index + "/_doc/1";

        try (GenericRestClient client = cluster.getRestClient(DLS_USER)) {
            GenericRestClient.HttpResponse response = client.putJson(doc1uri + "?refresh=true", DocNode.of("payload", "foo", "dept", "dept_e", "@timestamp", "2022-01-01T00:00:00Z"));
            Assert.assertEquals(response.getBody(), 201, response.getStatusCode());

            response = client.get(doc1uri);
            Assert.assertEquals(response.getBody(), 404, response.getStatusCode());
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get(doc1uri);
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            if(index.endsWith(LOGSDB_INDEX_POSTFIX)) {
                assertThat(TestData.getIndexMode(client, index), equalTo("logsdb"));
            } else {
                // null means default mode which is currently normal
                assertThat(TestData.getIndexMode(client, index), anyOf(equalTo("normal"), nullValue()));
            }
        }
    }

}
