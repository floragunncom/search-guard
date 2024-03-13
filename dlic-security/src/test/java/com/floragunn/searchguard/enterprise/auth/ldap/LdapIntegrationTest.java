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

package com.floragunn.searchguard.enterprise.auth.ldap;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Authc;
import com.floragunn.searchguard.test.TestSgConfig.Authc.Domain.AdditionalUserInformation;
import com.floragunn.searchguard.test.TestSgConfig.Authc.Domain.UserMapping;
import com.floragunn.searchguard.test.helper.certificate.TestCertificate;
import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

public class LdapIntegrationTest {
    // @ClassRule
    // public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    static TestCertificates certificatesContext = TestCertificates.builder().build();

    static TestCertificate ldapServerCertificate = certificatesContext.create("CN=ldap.example.com,OU=MyOU,O=MyO");

    static String PICTURE_OF_THORE = "U2VhcmNoIEd1YXJk==";

    static TestLdapDirectory.Entry KARLOTTA = new TestLdapDirectory.Entry("cn=Karlotta,ou=people,o=TEST").cn("Karlotta").uid("karlotta")
            .userpassword("karlottas-secret").displayName("Karlotta Karl").objectClass("inetOrgPerson");

    static TestLdapDirectory.Entry THORE = new TestLdapDirectory.Entry("cn=Thore,ou=people,o=TEST").cn("Thore").uid("tho").userpassword("tho-secret")
            .objectClass("inetOrgPerson").attr("departmentnumber", "a", "b").attr("jpegPhoto", PICTURE_OF_THORE).attr("businessCategory", "bc_1");

    static TestLdapDirectory.Entry PAUL = new TestLdapDirectory.Entry("cn=Paul,ou=people,o=TEST").cn("Paul").uid("paule").userpassword("p-secret")
            .objectClass("inetOrgPerson");

    static TestLdapDirectory.Entry TILDA_ADDITIONAL_USER_INFORMATION_ENTRY = new TestLdapDirectory.Entry("cn=Tilda,ou=people,o=TEST").cn("Tilda")
            .uid("tilda_additional_user_information").userpassword("p-undefined").objectClass("inetOrgPerson");

    static TestLdapDirectory.Entry ALL_ACCESS_GROUP = new TestLdapDirectory.Entry("cn=all_access,ou=groups,o=TEST").cn("all_access")
            .objectClass("groupOfUniqueNames").uniqueMember(KARLOTTA);

    static TestLdapDirectory.Entry STD_ACCESS_GROUP = new TestLdapDirectory.Entry("cn=std_access,ou=groups,o=TEST").cn("std_access")
            .objectClass("groupOfUniqueNames").attr("description", "My Description").attr("businessCategory", "x").uniqueMember(THORE);

    static TestLdapDirectory.Entry BUSINESS_CATEGORY_1_GROUP = new TestLdapDirectory.Entry("cn=bc_1,ou=groups,o=TEST").cn("bc_1")
            .objectClass("groupOfUniqueNames").attr("businessCategory", "bc_1");

    static TestLdapDirectory.Entry RECURSIVE_GROUP_1 = new TestLdapDirectory.Entry("cn=recursive1,ou=groups,o=TEST").cn("recursive1")
            .objectClass("groupOfUniqueNames").attr("businessCategory", "c").uniqueMember(PAUL, TILDA_ADDITIONAL_USER_INFORMATION_ENTRY);

    static TestLdapDirectory.Entry RECURSIVE_GROUP_2 = new TestLdapDirectory.Entry("cn=recursive2,ou=groups,o=TEST").cn("recursive2")
            .objectClass("groupOfUniqueNames").attr("businessCategory", "d").uniqueMember(RECURSIVE_GROUP_1);

    static TestLdapDirectory.Entry RECURSIVE_GROUP_3 = new TestLdapDirectory.Entry("cn=recursive3,ou=groups,o=TEST").cn("recursive3")
            .objectClass("groupOfUniqueNames").attr("businessCategory", "e").uniqueMember(RECURSIVE_GROUP_1);

    static TestLdapServer tlsLdapServer = TestLdapServer.with(TestLdapDirectory.BASE, KARLOTTA, THORE, PAUL, TILDA_ADDITIONAL_USER_INFORMATION_ENTRY,
            ALL_ACCESS_GROUP, STD_ACCESS_GROUP, RECURSIVE_GROUP_1, RECURSIVE_GROUP_2, RECURSIVE_GROUP_3, BUSINESS_CATEGORY_1_GROUP)
            .tls(ldapServerCertificate).build();

    static TestSgConfig.User TILDA_ADDITIONAL_USER_INFORMATION_USER = new TestSgConfig.User("tilda_additional_user_information")
            .roles(new TestSgConfig.Role("role").clusterPermissions("*"));

    static TestSgConfig.Role INDEX_PATTERN_WITH_ATTR = new TestSgConfig.Role("sg_index_pattern_with_attr_role")//
            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO")//
            .indexPermissions("SGS_CRUD").on("/attr_test_${user.attrs.pattern|toRegexFragment}/");

    static TestSgConfig.Role INDEX_PATTERN_WITH_ATTR_FOR_RECURSIVE_GROUPS = new TestSgConfig.Role(
            "sg_index_pattern_with_attr_role_for_recursive_groups")//
                    .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO")//
                    .indexPermissions("SGS_CRUD").on("/attr_test_${user.attrs.pattern_rec|toRegexFragment}/");

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
                    .skipIps("127.0.0.16/30")// 
                    .userMapping(new UserMapping()//
                            .attrsFrom("pattern", "ldap_user_entry.departmentnumber")//
                            .attrsFrom("pattern_rec", "ldap_group_entries[*].businessCategory[*]")), //
            new Authc.Domain("basic/ldap")//
                    .description("using by_attribute filter queries and getting user name from ldap_user_entry.displayName")//
                    .backend(DocNode.of(//
                            "idp.hosts", "#{var:ldapHost}", //
                            "idp.tls.trusted_cas", certificatesContext.getCaCertificate().getCertificateString(), //
                            "idp.tls.verify_hostnames", false, //
                            "user_search.filter.by_attribute", "uid", //
                            "group_search.base_dn", TestLdapDirectory.GROUPS.getDn(), //
                            "group_search.filter.by_attribute", "uniqueMember", //
                            "group_search.role_name_attribute", "dn", //
                            "group_search.recursive.enabled", true))
                    .acceptIps("127.0.0.17")//
                    .userMapping(new UserMapping()//
                            .userNameFromBackend("ldap_user_entry.displayName")//
                            .attrsFrom("pattern", "ldap_user_entry.departmentnumber")//
                            .attrsFrom("pattern_rec", "ldap_group_entries[*].businessCategory[*]")), //   
            new Authc.Domain("basic/ldap")//
                    .description("group search based on attribute of ldap_user_entry")//
                    .backend(DocNode.of(//
                            "idp.hosts", "#{var:ldapHost}", //
                            "idp.tls.trusted_cas", certificatesContext.getCaCertificate().getCertificateString(), //
                            "idp.tls.verify_hostnames", false, //
                            "user_search.filter.by_attribute", "uid", //
                            "group_search.base_dn", TestLdapDirectory.GROUPS.getDn(), //
                            "group_search.filter.raw", "(businessCategory=${ldap_user_entry.businessCategory})", //
                            "group_search.role_name_attribute", "dn", //
                            "group_search.recursive.enabled", true))
                    .acceptIps("127.0.0.18")//
                    .userMapping(new UserMapping()//
                            .attrsFrom("pattern", "ldap_user_entry.departmentnumber")//
                            .attrsFrom("pattern_rec", "ldap_group_entries[*].businessCategory[*]")), //     
            new Authc.Domain("basic/ldap")//
                    .description("using retrieve_attributes setting")//
                    .backend(DocNode.of(//
                            "idp.hosts", "#{var:ldapHost}", //
                            "idp.tls.trusted_cas", certificatesContext.getCaCertificate().getCertificateString(), //
                            "idp.tls.verify_hostnames", false, //
                            "user_search.filter.by_attribute", "uid", //
                            "user_search.retrieve_attributes", "uid", //
                            "group_search.base_dn", TestLdapDirectory.GROUPS.getDn(), //
                            "group_search.filter.by_attribute", "uniqueMember", //
                            "group_search.role_name_attribute", "dn", //
                            "group_search.retrieve_attributes", "businessCategory", //
                            "group_search.recursive.enabled", true))
                    .acceptIps("127.0.0.19")//
                    .userMapping(new UserMapping()//
                            .userNameFromBackend("ldap_user_entry.uid")), //
            new Authc.Domain("basic/internal_users_db")//
                    .additionalUserInformation(new AdditionalUserInformation("ldap", DocNode.of(//
                            "idp.hosts", "#{var:ldapHost}", //
                            "idp.tls.trusted_cas", certificatesContext.getCaCertificate().getCertificateString(), //
                            "idp.tls.verify_hostnames", false, //
                            "user_search.filter.raw", "(uid=${user.name})", //
                            "group_search.base_dn", TestLdapDirectory.GROUPS.getDn(), //
                            "group_search.filter.raw", "(uniqueMember=${dn})", //
                            "group_search.role_name_attribute", "dn", //
                            "group_search.recursive.enabled", true)))
                    .userMapping(new UserMapping()//
                            .attrsFrom("pattern", "ldap_user_entry.departmentnumber")//
                            .attrsFrom("pattern_rec", "ldap_group_entries[*].businessCategory[*]"))

    ).debug().userCacheEnabled(false);

    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().singleNode().sslEnabled().enterpriseModulesEnabled().resources("ldap")//
            .roles(TestSgConfig.Role.ALL_ACCESS, INDEX_PATTERN_WITH_ATTR, INDEX_PATTERN_WITH_ATTR_FOR_RECURSIVE_GROUPS)//
            .roleToRoleMapping(TestSgConfig.Role.ALL_ACCESS, ALL_ACCESS_GROUP.getDn())//
            .roleToRoleMapping(INDEX_PATTERN_WITH_ATTR, STD_ACCESS_GROUP.getDn())//
            .roleToRoleMapping(INDEX_PATTERN_WITH_ATTR_FOR_RECURSIVE_GROUPS, RECURSIVE_GROUP_3.getDn())//
            .authc(AUTHC).users(TILDA_ADDITIONAL_USER_INFORMATION_USER).var("ldapHost", () -> tlsLdapServer.hostAndPort()).embedded().build();

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
    public void name_fromLdapEntry() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(KARLOTTA)) {
            client.setLocalAddress(InetAddress.getByAddress(new byte[] { 127, 0, 0, 17 }));

            GenericRestClient.HttpResponse response = client.get("/_searchguard/authinfo");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), "Karlotta Karl", response.getBodyAsDocNode().get("user_name"));
        }
    }

    @Test
    public void roles() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(KARLOTTA)) {
            client.setLocalAddress(InetAddress.getByAddress(new byte[] { 127, 0, 0, 17 }));

            GenericRestClient.HttpResponse response = client.get("/_searchguard/authinfo");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), Arrays.asList("cn=all_access,ou=groups,o=TEST"),
                    response.getBodyAsDocNode().get("backend_roles"));
        }
    }

    @Test
    public void roles_rawQuery() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(KARLOTTA)) {
            GenericRestClient.HttpResponse response = client.get("/_searchguard/authinfo");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), Arrays.asList("cn=all_access,ou=groups,o=TEST"),
                    response.getBodyAsDocNode().get("backend_roles"));
        }
    }

    @Test
    public void roles_groupSearchWithLdapEntry() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(THORE)) {
            client.setLocalAddress(InetAddress.getByAddress(new byte[] { 127, 0, 0, 18 }));

            GenericRestClient.HttpResponse response = client.get("/_searchguard/authinfo");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), Arrays.asList("cn=bc_1,ou=groups,o=TEST"), response.getBodyAsDocNode().get("backend_roles"));
        }
    }

    @Test
    public void attributeIntegrationTest() throws Exception {

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(KARLOTTA)) {
            SearchResponse searchResponse = client.search(
                    new SearchRequest("attr_test_*").source(new SearchSourceBuilder().size(100).query(QueryBuilders.matchAllQuery())),
                    RequestOptions.DEFAULT);

            Assert.assertEquals(5, searchResponse.getHits().getTotalHits().value);
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(THORE)) {
            SearchResponse searchResponse = client.search(
                    new SearchRequest("attr_test_*").source(new SearchSourceBuilder().size(100).query(QueryBuilders.matchAllQuery())),
                    RequestOptions.DEFAULT);

            Assert.assertEquals(2, searchResponse.getHits().getTotalHits().value);
        }

    }

    @Test
    public void attributeIntegrationTest_recursiveGroups() throws Exception {

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(PAUL)) {
            SearchResponse searchResponse = client.search(
                    new SearchRequest("attr_test_*").source(new SearchSourceBuilder().size(100).query(QueryBuilders.matchAllQuery())),
                    RequestOptions.DEFAULT);

            Assert.assertEquals(3, searchResponse.getHits().getTotalHits().value);
        }

    }

    @Test
    public void additionalUserInformation() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(TILDA_ADDITIONAL_USER_INFORMATION_USER)) {
            GenericRestClient.HttpResponse response = client.get("/_searchguard/authinfo");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), TILDA_ADDITIONAL_USER_INFORMATION_USER.getName(), response.getBodyAsDocNode().get("user_name"));
            Assert.assertEquals(response.getBody(), ImmutableSet.of(RECURSIVE_GROUP_1.getDn(), RECURSIVE_GROUP_2.getDn(), RECURSIVE_GROUP_3.getDn()),
                    ImmutableSet.of((Collection<?>) response.getBodyAsDocNode().get("backend_roles")));
            Assert.assertTrue(response.getBody(),
                    ((Collection<?>) response.getBodyAsDocNode().get("sg_roles")).contains("user_tilda_additional_user_information__role"));
            Assert.assertEquals(response.getBody(), Arrays.asList("pattern_rec"), response.getBodyAsDocNode().get("attribute_names"));
        }
    }

    @Test
    public void testAuthDomainInfo() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(KARLOTTA)) {
            HttpResponse response = restClient.get("/_searchguard/authinfo");
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsString("user").startsWith("User karlotta <basic/ldap>"));
        }
    }

    @Test
    public void wrongPassword() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(KARLOTTA.getName(), "wrong-password")) {
            GenericRestClient.HttpResponse response = client.get("/_searchguard/authinfo");
            Assert.assertEquals(response.getBody(), 401, response.getStatusCode());
        }
    }

    @Test
    public void userNotFound() throws Exception {
        try (GenericRestClient client = cluster.getRestClient("unknown-user", "password")) {
            GenericRestClient.HttpResponse response = client.get("/_searchguard/authinfo");
            Assert.assertEquals(response.getBody(), 401, response.getStatusCode());
        }
    }

    @Test
    public void retrieveAttributes() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(THORE)) {
            GenericRestClient.HttpResponse response = client.get("/_searchguard/auth/debug");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), Arrays.asList(PICTURE_OF_THORE), response.getBodyAsDocNode().findByJsonPath(
                    "$.debug[?(@.method=='basic/ldap' && @.message=='Backends successful')].details.user_mapping_attributes.ldap_user_entry.jpegPhoto[0]"));
            Assert.assertEquals(response.getBody(), Arrays.asList("My Description"), response.getBodyAsDocNode().findByJsonPath(
                    "$.debug[?(@.method=='basic/ldap' && @.message=='Backends successful')].details.user_mapping_attributes.ldap_group_entries[*].description[0]"));
            Assert.assertEquals(response.getBody(), Arrays.asList("x"), response.getBodyAsDocNode().findByJsonPath(
                    "$.debug[?(@.method=='basic/ldap' && @.message=='Backends successful')].details.user_mapping_attributes.ldap_group_entries[*].businessCategory[0]"));
        }

        try (GenericRestClient client = cluster.getRestClient(THORE)) {
            client.setLocalAddress(InetAddress.getByAddress(new byte[] { 127, 0, 0, 19 }));

            GenericRestClient.HttpResponse response = client.get("/_searchguard/auth/debug");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), Collections.emptyList(), response.getBodyAsDocNode().findByJsonPath(
                    "$.debug[?(@.method=='basic/ldap' && @.message=='Backends successful')].details.user_mapping_attributes.ldap_user_entry.jpegPhoto[0]"));
            Assert.assertEquals(response.getBody(), Collections.emptyList(), response.getBodyAsDocNode().findByJsonPath(
                    "$.debug[?(@.method=='basic/ldap' && @.message=='Backends successful')].details.user_mapping_attributes.ldap_group_entries[*].description[0]"));
            Assert.assertEquals(response.getBody(), Arrays.asList("x"), response.getBodyAsDocNode().findByJsonPath(
                    "$.debug[?(@.method=='basic/ldap' && @.message=='Backends successful')].details.user_mapping_attributes.ldap_group_entries[*].businessCategory[0]"));
        }
    }
}
