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

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

public class DlsWriteIntTest {
    static final TestSgConfig.User ADMIN = new TestSgConfig.User("admin")
            .roles(new Role("all_access").indexPermissions("*").on("*").clusterPermissions("*"));
    static final TestSgConfig.User DLS_USER = new TestSgConfig.User("dls_user")
            .roles(new Role("role").indexPermissions("SGS_MANAGE", "SGS_CRUD").dls(DocNode.of("term.dept.value", "dept_d")).on("dls_*").clusterPermissions("*"));

    static final TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db"));
    static final TestSgConfig.DlsFls DLSFLS = new TestSgConfig.DlsFls().metrics("detailed");
    public static final String LOGSDB_INDEX_POSTFIX = "logsdb";

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().enterpriseModulesEnabled().authc(AUTHC).dlsFls(DLSFLS)
            .users(ADMIN, DLS_USER).resources("dlsfls").build();

    @Test
    public void newIndex_allowedRead() throws Exception {
        String index = "/dls_new_index_allowed_read";
        String doc1uri = index + "/_doc/1";

        try (GenericRestClient client = cluster.getRestClient(DLS_USER)) {
            GenericRestClient.HttpResponse response = client.putJson(doc1uri + "?refresh=true", DocNode.of("payload", "foo", "dept", "dept_d"));
            Assert.assertEquals(response.getBody(), 201, response.getStatusCode());

            response = client.get(doc1uri);
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get(doc1uri);
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
        }
    }

    @Test
    public void newIndex_disallowedRead() throws Exception {
        String index = "/dls_new_index_disallowed_read";
        String doc1uri = index + "/_doc/1";

        try (GenericRestClient client = cluster.getRestClient(DLS_USER)) {
            GenericRestClient.HttpResponse response = client.putJson(doc1uri + "?refresh=true", DocNode.of("payload", "foo", "dept", "dept_e"));
            Assert.assertEquals(response.getBody(), 201, response.getStatusCode());

            response = client.get(doc1uri);
            Assert.assertEquals(response.getBody(), 404, response.getStatusCode());
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get(doc1uri);
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
        }
    }

}
