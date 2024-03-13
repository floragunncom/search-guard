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

package com.floragunn.searchguard.enterprise.license;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Authc;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

public class LicenseInfoIntegrationTest {
    static TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(//
            new Authc.Domain("basic/ldap")//
                    .backend(DocNode.of(//
                            "idp.hosts", "localhost")),
            new Authc.Domain("basic/internal_users_db")//
    );

    static TestSgConfig.User ADMIN = new TestSgConfig.User("admin").roles(new TestSgConfig.Role("role").clusterPermissions("*"));

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().enterpriseModulesEnabled().authc(AUTHC).users(ADMIN)
            .embedded().build();

    @Test
    public void basicTest() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            HttpResponse response = client.get("/_searchguard/license/info");

            System.out.println(response.getBody());
            Assert.assertEquals(response.getBody(), ImmutableSet.of("authentication_backend/ldap", "dlsfls", "dlsfls_legacy"),
                    ImmutableSet.of(response.getBodyAsDocNode().getAsNode("licenses_required").getAsNode("enterprise").toListOfStrings()));
        }
    }
}
