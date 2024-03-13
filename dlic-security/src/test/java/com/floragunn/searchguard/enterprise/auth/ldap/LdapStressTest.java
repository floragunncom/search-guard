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

package com.floragunn.searchguard.enterprise.auth.ldap;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Authc;
import com.floragunn.searchguard.test.TestSgConfig.Authc.Domain.UserMapping;
import com.floragunn.searchguard.test.helper.certificate.TestCertificate;
import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

/**
 * Long running LDAP integration tests. Disabled by default.
 */
@Ignore
public class LdapStressTest {
    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    static TestCertificates certificatesContext = TestCertificates.builder().build();

    static TestCertificate ldapServerCertificate = certificatesContext.create("CN=ldap.example.com,OU=MyOU,O=MyO");

    static TestLdapDirectory.Entry KARLOTTA = new TestLdapDirectory.Entry("cn=Karlotta,ou=people,o=TEST").cn("Karlotta").uid("karlotta")
            .userpassword("karlottas-secret").displayName("Karlotta Karl").objectClass("inetOrgPerson");

    static TestLdapDirectory.Entry THORE = new TestLdapDirectory.Entry("cn=Thore,ou=people,o=TEST").cn("Thore").uid("tho").userpassword("tho-secret")
            .objectClass("inetOrgPerson").attr("departmentnumber", "a", "b").attr("businessCategory", "bc_1");

    static TestLdapDirectory.Entry PAUL = new TestLdapDirectory.Entry("cn=Paul,ou=people,o=TEST").cn("Paul").uid("paule").userpassword("p-secret")
            .objectClass("inetOrgPerson");

    static TestLdapDirectory.Entry ALL_ACCESS_GROUP = new TestLdapDirectory.Entry("cn=all_access,ou=groups,o=TEST").cn("all_access")
            .objectClass("groupOfUniqueNames").uniqueMember(KARLOTTA);

    static TestLdapDirectory.Entry STD_ACCESS_GROUP = new TestLdapDirectory.Entry("cn=std_access,ou=groups,o=TEST").cn("std_access")
            .objectClass("groupOfUniqueNames").uniqueMember(THORE);

    static TestLdapServer tlsLdapServer = TestLdapServer.with(TestLdapDirectory.BASE, KARLOTTA, THORE, PAUL, ALL_ACCESS_GROUP, STD_ACCESS_GROUP)
            .tls(ldapServerCertificate).bindRequestDelay(Duration.ofSeconds(5)).build();

    static TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(//
            new Authc.Domain("basic/ldap")//
                    .description("using raw filter queries")//
                    .backend(DocNode.of(//
                            "idp.hosts", "#{var:ldapHost}", //
                            "idp.tls.trusted_cas", certificatesContext.getCaCertificate().getCertificateString(), //
                            "idp.tls.verify_hostnames", false, //
                            "user_search.filter.raw", "(uid=${user.name})", //
                            "group_search.base_dn", TestLdapDirectory.GROUPS.getDn(), //
                            "group_search.filter.raw", "(uniqueMember=${dn})", //
                            "group_search.role_name_attribute", "dn", //
                            "group_search.recursive.enabled", true))
                    .userMapping(new UserMapping()//
                            .attrsFrom("pattern", "ldap_user_entry.departmentnumber")//
                            .attrsFrom("pattern_rec", "ldap_group_entries[*].businessCategory[*]")) //
    );

    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().singleNode().sslEnabled().enterpriseModulesEnabled().resources("ldap")//
            .roles(TestSgConfig.Role.ALL_ACCESS)//
            .roleToRoleMapping(TestSgConfig.Role.ALL_ACCESS, ALL_ACCESS_GROUP.getDn())//
            .authc(AUTHC).var("ldapHost", () -> tlsLdapServer.hostAndPort()).embedded().build();

    @ClassRule
    public static TestRule serverChain = RuleChain.outerRule(tlsLdapServer).around(cluster);

    @BeforeClass
    public static void initTestData() {
        try (Client tc = cluster.getInternalNodeClient()) {

            tc.index(new IndexRequest("attr_test_a").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"filter_attr\": \"a\", \"amount\": 1010}",
                    XContentType.JSON)).actionGet();
            tc.index(new IndexRequest("attr_test_b").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"filter_attr\": \"b\", \"amount\": 2020}",
                    XContentType.JSON)).actionGet();
            tc.index(new IndexRequest("attr_test_c").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"filter_attr\": \"c\", \"amount\": 3030}",
                    XContentType.JSON)).actionGet();
            tc.index(new IndexRequest("attr_test_d").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"filter_attr\": \"d\", \"amount\": 4040}",
                    XContentType.JSON)).actionGet();
            tc.index(new IndexRequest("attr_test_e").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"filter_attr\": \"e\", \"amount\": 5050}",
                    XContentType.JSON)).actionGet();
        }
    }

    @Test
    public void concurrentAuth() {
        int count = 15;
        
        CompletableFuture<?> [] futures = new CompletableFuture<?>[count];
        ExecutorService executorService = Executors.newFixedThreadPool(count);
        
        for (int i = 0; i < count; i++) {
            futures[i] = CompletableFuture.supplyAsync(() -> {
                try (GenericRestClient client = cluster.getRestClient(KARLOTTA)) {
                    GenericRestClient.HttpResponse response = client.get("/_searchguard/authinfo");
                    Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
                    Assert.assertEquals(response.getBody(), "karlotta", response.getBodyAsDocNode().get("user_name"));
                    return null;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }                
            }, executorService);
        }
        
        CompletableFuture.allOf(futures).join();        
    }

}
