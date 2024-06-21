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

package com.floragunn.searchguard.enterprise.auditlog.integration;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.enterprise.auditlog.impl.AuditMessage;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.BearerAuthorization;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchsupport.junit.AsyncAssert;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AuditlogIntegrationTest {

    static final TestSgConfig.User AUDIT_IGNORED_USER = new TestSgConfig.User("audit_ignored_user")
            .roles(new TestSgConfig.Role("all_access").indexPermissions("*").on("*").clusterPermissions("*"));
    static final TestSgConfig.User USER = new TestSgConfig.User("user")
            .roles(new TestSgConfig.Role("all_access").indexPermissions("*").on("*").clusterPermissions("*"));

    static TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(
            new TestSgConfig.Authc.Domain("basic/internal_users_db")
    );

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder()
            .users(AUDIT_IGNORED_USER, USER)
            .authc(AUTHC)
            .sslEnabled()
            .enterpriseModulesEnabled()
            .nodeSettings(
                    ConfigConstants.SEARCHGUARD_AUDIT_TYPE_DEFAULT, TestAuditlogImpl.class.getName(),
                    ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, true,
                    ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_REST, true,
                    ConfigConstants.SEARCHGUARD_AUDIT_THREADPOOL_SIZE, 0,
                    ConfigConstants.SEARCHGUARD_AUDIT_IGNORE_USERS, Collections.singletonList(AUDIT_IGNORED_USER.getName())
            )
            .embedded().build();

    @Test
    public void testAuditLog_kibanaLogin() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient()) {
            TestAuditlogImpl.clear();

            DocNode requestBody = DocNode.of("user", USER.getName(), "password", USER.getPassword());
            GenericRestClient.HttpResponse response = restClient.postJson("/_searchguard/auth/session", requestBody);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_CREATED));

            AsyncAssert.awaitAssert("Messages arrived", () -> getMessagesByCategory(AuditMessage.Category.KIBANA_LOGIN).size() == 1, Duration.ofSeconds(2));
            DocNode auditMessage = DocNode.parse(Format.JSON).from(getMessagesByCategory(AuditMessage.Category.KIBANA_LOGIN).get(0).toJson());

            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.CATEGORY), equalTo(AuditMessage.Category.KIBANA_LOGIN.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.CLUSTER_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.NODE_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.ORIGIN), equalTo(AuditLog.Origin.REST.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.NODE_ID), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.REQUEST_LAYER), equalTo(AuditLog.Origin.REST.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.UTC_TIMESTAMP), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.FORMAT_VERSION), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.REMOTE_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.NODE_HOST_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.REQUEST_EFFECTIVE_USER), equalTo(requestBody.getAsString("user")));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.REQUEST_EFFECTIVE_USER_AUTH_DOMAIN), equalTo("basic/internal_users_db"));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.NODE_VERSION), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.NODE_HOST_NAME), notNullValue());

            TestAuditlogImpl.clear();

            response = restClient.postJson("/_searchguard/auth/session/with_header", DocNode.EMPTY, cluster.getBasicAuthHeader(USER.getName(), USER.getPassword()));
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            AsyncAssert.awaitAssert("Messages arrived", () -> getMessagesByCategory(AuditMessage.Category.KIBANA_LOGIN).size() == 1, Duration.ofSeconds(2));
            auditMessage = DocNode.parse(Format.JSON).from(getMessagesByCategory(AuditMessage.Category.KIBANA_LOGIN).get(0).toJson());

            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.CATEGORY), equalTo(AuditMessage.Category.KIBANA_LOGIN.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.CLUSTER_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.NODE_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.ORIGIN), equalTo(AuditLog.Origin.REST.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.NODE_ID), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.REQUEST_LAYER), equalTo(AuditLog.Origin.REST.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.UTC_TIMESTAMP), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.FORMAT_VERSION), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.REMOTE_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.NODE_HOST_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.REQUEST_EFFECTIVE_USER), equalTo(requestBody.getAsString("user")));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.REQUEST_EFFECTIVE_USER_AUTH_DOMAIN), equalTo("basic/internal_users_db"));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.NODE_VERSION), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.NODE_HOST_NAME), notNullValue());

        }
    }

    @Test
    public void testAuditLog_kibanaLogout() throws Exception {

        String token;

        try (GenericRestClient restClient = cluster.getRestClient()) {

            GenericRestClient.HttpResponse response = restClient.postJson("/_searchguard/auth/session/with_header", DocNode.EMPTY, cluster.getBasicAuthHeader(USER.getName(), USER.getPassword()));
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            token = response.getBodyAsDocNode().getAsString("token");

            Assert.assertNotNull(response.getBody(), token);
        }

        try (GenericRestClient restClient = cluster.getRestClient(new BearerAuthorization(token))) {
            TestAuditlogImpl.clear();

            GenericRestClient.HttpResponse response = restClient.delete("/_searchguard/auth/session");
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            AsyncAssert.awaitAssert("Messages arrived", () -> getMessagesByCategory(AuditMessage.Category.KIBANA_LOGOUT).size() == 1, Duration.ofSeconds(2));
            DocNode auditMessage = DocNode.parse(Format.JSON).from(getMessagesByCategory(AuditMessage.Category.KIBANA_LOGOUT).get(0).toJson());

            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.CATEGORY), equalTo(AuditMessage.Category.KIBANA_LOGOUT.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.CLUSTER_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.NODE_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.ORIGIN), equalTo(AuditLog.Origin.REST.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.NODE_ID), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.REQUEST_LAYER), equalTo(AuditLog.Origin.REST.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.UTC_TIMESTAMP), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.FORMAT_VERSION), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.REMOTE_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.NODE_HOST_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.REQUEST_EFFECTIVE_USER), equalTo(USER.getName()));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.REQUEST_EFFECTIVE_USER_AUTH_DOMAIN), nullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.NODE_VERSION), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.NODE_HOST_NAME), notNullValue());

        }
    }

    @Test
    public void testAuditLog_composableIndexTemplateWriteHistory() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient(USER).trackResources()) {
            TestAuditlogImpl.clear();

            String templateName = "test-composable-template";
            DocNode template = DocNode.of("index_patterns", Collections.singletonList("test-composable"));

            GenericRestClient.HttpResponse response = restClient.putJson("/_index_template/" + templateName, template);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            AsyncAssert.awaitAssert("Messages arrived", () -> getMessagesByCategory(AuditMessage.Category.INDEX_TEMPLATE_WRITE).size() == 1, Duration.ofSeconds(2));
            DocNode auditMessage = DocNode.parse(Format.JSON).from(getMessagesByCategory(AuditMessage.Category.INDEX_TEMPLATE_WRITE).get(0).toJson());

            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.CATEGORY), equalTo(AuditMessage.Category.INDEX_TEMPLATE_WRITE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.COMPLIANCE_OPERATION), equalTo(AuditLog.Operation.CREATE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDEX_TEMPLATES), hasSize(1));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDEX_TEMPLATES).get(0), equalTo(templateName));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.CLUSTER_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.ORIGIN), equalTo(AuditLog.Origin.REST.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_LAYER), equalTo(AuditLog.Origin.TRANSPORT.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_BODY), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_ID), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.UTC_TIMESTAMP), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.FORMAT_VERSION), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REMOTE_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_HOST_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_EFFECTIVE_USER), equalTo(USER.getName()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_EFFECTIVE_USER_AUTH_DOMAIN), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.FORMAT_VERSION), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_HOST_NAME), notNullValue());

            TestAuditlogImpl.clear();

            template = template.with("index_patterns", Collections.singletonList("composable-new-pattern"));

            response = restClient.putJson("/_index_template/" + templateName, template);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            AsyncAssert.awaitAssert("Messages arrived", () -> getMessagesByCategory(AuditMessage.Category.INDEX_TEMPLATE_WRITE).size() == 1, Duration.ofSeconds(2));
            auditMessage = DocNode.parse(Format.JSON).from(getMessagesByCategory(AuditMessage.Category.INDEX_TEMPLATE_WRITE).get(0).toJson());

            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.CATEGORY), equalTo(AuditMessage.Category.INDEX_TEMPLATE_WRITE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.COMPLIANCE_OPERATION), equalTo(AuditLog.Operation.UPDATE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDEX_TEMPLATES), hasSize(1));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDEX_TEMPLATES).get(0), equalTo(templateName));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.CLUSTER_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.ORIGIN), equalTo(AuditLog.Origin.REST.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_LAYER), equalTo(AuditLog.Origin.TRANSPORT.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_BODY), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_ID), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.UTC_TIMESTAMP), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.FORMAT_VERSION), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REMOTE_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_HOST_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_EFFECTIVE_USER), equalTo(USER.getName()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_EFFECTIVE_USER_AUTH_DOMAIN), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.FORMAT_VERSION), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_HOST_NAME), notNullValue());

            TestAuditlogImpl.clear();

            response = restClient.delete("/_index_template/" + templateName);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            AsyncAssert.awaitAssert("Messages arrived", () -> getMessagesByCategory(AuditMessage.Category.INDEX_TEMPLATE_WRITE).size() == 1, Duration.ofSeconds(2));
            auditMessage = DocNode.parse(Format.JSON).from(getMessagesByCategory(AuditMessage.Category.INDEX_TEMPLATE_WRITE).get(0).toJson());

            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.CATEGORY), equalTo(AuditMessage.Category.INDEX_TEMPLATE_WRITE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.COMPLIANCE_OPERATION), equalTo(AuditLog.Operation.DELETE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDEX_TEMPLATES), hasSize(1));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDEX_TEMPLATES).get(0), equalTo(templateName));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.CLUSTER_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.ORIGIN), equalTo(AuditLog.Origin.REST.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_LAYER), equalTo(AuditLog.Origin.TRANSPORT.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_ID), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.UTC_TIMESTAMP), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.FORMAT_VERSION), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REMOTE_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_HOST_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_EFFECTIVE_USER), equalTo(USER.getName()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_EFFECTIVE_USER_AUTH_DOMAIN), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.FORMAT_VERSION), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_HOST_NAME), notNullValue());
        }
    }

    @Test
    public void testAuditLog_legacyIndexTemplateWriteHistory() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient(USER).trackResources()){
            TestAuditlogImpl.clear();

            String templateName = "test-legacy-template";
            DocNode template = DocNode.of("index_patterns", Collections.singletonList("test-legacy"));

            GenericRestClient.HttpResponse response = restClient.putJson("/_template/" + templateName, template);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            AsyncAssert.awaitAssert("Messages arrived", () -> getMessagesByCategory(AuditMessage.Category.INDEX_TEMPLATE_WRITE).size() == 1, Duration.ofSeconds(2));
            DocNode auditMessage = DocNode.parse(Format.JSON).from(getMessagesByCategory(AuditMessage.Category.INDEX_TEMPLATE_WRITE).get(0).toJson());

            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.CATEGORY), equalTo(AuditMessage.Category.INDEX_TEMPLATE_WRITE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.COMPLIANCE_OPERATION), equalTo(AuditLog.Operation.CREATE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDEX_TEMPLATES), hasSize(1));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDEX_TEMPLATES).get(0), equalTo(templateName));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.CLUSTER_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.ORIGIN), equalTo(AuditLog.Origin.REST.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_LAYER), equalTo(AuditLog.Origin.TRANSPORT.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_BODY), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_ID), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.UTC_TIMESTAMP), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.FORMAT_VERSION), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REMOTE_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_HOST_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_EFFECTIVE_USER), equalTo(USER.getName()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_EFFECTIVE_USER_AUTH_DOMAIN), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.FORMAT_VERSION), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_HOST_NAME), notNullValue());

            TestAuditlogImpl.clear();

            template = template.with("index_patterns", Collections.singletonList("legacy-new-pattern"));

            response = restClient.putJson("/_template/" + templateName, template);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            AsyncAssert.awaitAssert("Messages arrived", () -> getMessagesByCategory(AuditMessage.Category.INDEX_TEMPLATE_WRITE).size() == 1, Duration.ofSeconds(2));
            auditMessage = DocNode.parse(Format.JSON).from(getMessagesByCategory(AuditMessage.Category.INDEX_TEMPLATE_WRITE).get(0).toJson());

            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.CATEGORY), equalTo(AuditMessage.Category.INDEX_TEMPLATE_WRITE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.COMPLIANCE_OPERATION), equalTo(AuditLog.Operation.UPDATE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDEX_TEMPLATES), hasSize(1));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDEX_TEMPLATES).get(0), equalTo(templateName));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.CLUSTER_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.ORIGIN), equalTo(AuditLog.Origin.REST.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_LAYER), equalTo(AuditLog.Origin.TRANSPORT.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_BODY), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_ID), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.UTC_TIMESTAMP), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.FORMAT_VERSION), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REMOTE_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_HOST_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_EFFECTIVE_USER), equalTo(USER.getName()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_EFFECTIVE_USER_AUTH_DOMAIN), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.FORMAT_VERSION), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_HOST_NAME), notNullValue());

            TestAuditlogImpl.clear();

            response = restClient.delete("/_template/" + templateName);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            AsyncAssert.awaitAssert("Messages arrived", () -> getMessagesByCategory(AuditMessage.Category.INDEX_TEMPLATE_WRITE).size() == 1, Duration.ofSeconds(2));
            auditMessage = DocNode.parse(Format.JSON).from(getMessagesByCategory(AuditMessage.Category.INDEX_TEMPLATE_WRITE).get(0).toJson());

            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.CATEGORY), equalTo(AuditMessage.Category.INDEX_TEMPLATE_WRITE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.COMPLIANCE_OPERATION), equalTo(AuditLog.Operation.DELETE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDEX_TEMPLATES), hasSize(1));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDEX_TEMPLATES).get(0), equalTo(templateName));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.CLUSTER_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.ORIGIN), equalTo(AuditLog.Origin.REST.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_LAYER), equalTo(AuditLog.Origin.TRANSPORT.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_ID), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.UTC_TIMESTAMP), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.FORMAT_VERSION), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REMOTE_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_HOST_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_EFFECTIVE_USER), equalTo(USER.getName()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_EFFECTIVE_USER_AUTH_DOMAIN), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.FORMAT_VERSION), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_HOST_NAME), notNullValue());
        }
    }

    @Test
    public void testAuditLog_composableIndexTemplateWriteHistory_noLogWhenActionFails() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient(USER).trackResources()) {
            TestAuditlogImpl.clear();

            String templateName = "test-composable-template";
            DocNode template = DocNode.of("index_patterns", Collections.singletonList("test-composable"), "template", DocNode.of("settings", DocNode.of("number_of_shards", 0)));

            GenericRestClient.HttpResponse response = restClient.putJson("/_index_template/" + templateName, template);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));

            response = restClient.delete("/_index_template/" + 1 + templateName);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));

            Thread.sleep(1000);
            assertThat(TestAuditlogImpl.sb.toString(), getMessagesByCategory(AuditMessage.Category.INDEX_TEMPLATE_WRITE), hasSize(0));
        }
    }

    @Test
    public void testAuditLog_legacyIndexTemplateWriteHistory_noLogWhenActionFails() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient(USER).trackResources()) {
            TestAuditlogImpl.clear();

            String templateName = "test-legacy-template";
            DocNode template = DocNode.of("index_patterns", Collections.singletonList("test-legacy"), "settings", DocNode.of("number_of_shards", 0));

            GenericRestClient.HttpResponse response = restClient.putJson("/_template/" + templateName, template);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));

            response = restClient.delete("/_template/" + 1 + templateName);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));

            Thread.sleep(1000);
            assertThat(TestAuditlogImpl.sb.toString(), getMessagesByCategory(AuditMessage.Category.INDEX_TEMPLATE_WRITE), hasSize(0));
        }
    }

    @Test
    public void testAuditLog_composableIndexTemplateWriteHistory_noLogWhenUserIsIgnored() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient(AUDIT_IGNORED_USER).trackResources()) {
            TestAuditlogImpl.clear();

            String templateName = "test-composable-template";
            DocNode template = DocNode.of("index_patterns", Collections.singletonList("test-composable"));

            GenericRestClient.HttpResponse response = restClient.putJson("/_index_template/" + templateName, template);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            response = restClient.putJson("/_index_template/" + templateName, template);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            response = restClient.delete("/_index_template/" + templateName);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            Thread.sleep(1000);
            assertThat(TestAuditlogImpl.sb.toString(), getMessagesByCategory(AuditMessage.Category.INDEX_TEMPLATE_WRITE), hasSize(0));
        }
    }

    @Test
    public void testAuditLog_legacyIndexTemplateWriteHistory_noLogWhenUserIsIgnored() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient(AUDIT_IGNORED_USER).trackResources()) {
            TestAuditlogImpl.clear();

            String templateName = "test-legacy-template";
            DocNode template = DocNode.of("index_patterns", Collections.singletonList("test-legacy"));

            GenericRestClient.HttpResponse response = restClient.putJson("/_template/" + templateName, template);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            response = restClient.putJson("/_template/" + templateName, template);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            response = restClient.delete("/_template/" + templateName);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            Thread.sleep(1000);
            assertThat(TestAuditlogImpl.sb.toString(), getMessagesByCategory(AuditMessage.Category.INDEX_TEMPLATE_WRITE), hasSize(0));
        }
    }

    @Test
    public void testAuditLog_indexWriteHistory_createOperation_simpleIndexName() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient(USER).trackResources()) {
            TestAuditlogImpl.clear();

            String indexPrefix = "test-index-simple-name";
            String indexName = indexPrefix + "-1";
            DocNode settings = DocNode.of("index", DocNode.of("number_of_shards", 3, "number_of_replicas", 2));
            DocNode aliases = DocNode.of(
                    "alias1", DocNode.EMPTY,
                    "alias2", DocNode.of("filter", DocNode.of("term", DocNode.of("doc", "1")), "index_routing", "shard1")
            );
            DocNode mappings = DocNode.of("properties", DocNode.of("field1", DocNode.of("type", "text")));
            DocNode reqBody = DocNode.of("settings", settings, "aliases", aliases, "mappings", mappings);

            GenericRestClient.HttpResponse response = restClient.putJson("/" + indexName, reqBody);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            AsyncAssert.awaitAssert("Messages arrived", () -> getMessagesByCategory(AuditMessage.Category.INDEX_WRITE).size() == 1, Duration.ofSeconds(2));
            DocNode auditMessage = DocNode.parse(Format.JSON).from(getMessagesByCategory(AuditMessage.Category.INDEX_WRITE).get(0).toJson());

            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.CATEGORY), equalTo(AuditMessage.Category.INDEX_WRITE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.COMPLIANCE_OPERATION), equalTo(AuditLog.Operation.CREATE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDICES), hasSize(1));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDICES).get(0), equalTo(indexName));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.RESOLVED_INDICES), nullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_BODY), notNullValue());

            DocNode msgReqBodyField = DocNode.parse(Format.JSON).from(auditMessage.getAsString(AuditMessage.REQUEST_BODY));
            assertThat(auditMessage.toJsonString(), msgReqBodyField, hasKey("index"));
            assertThat(auditMessage.toJsonString(), msgReqBodyField, hasKey("settings"));
            assertThat(auditMessage.toJsonString(), msgReqBodyField, hasKey("mappings"));
            assertThat(auditMessage.toJsonString(), msgReqBodyField, hasKey("aliases"));
            assertThat(auditMessage.toJsonString(), msgReqBodyField, hasKey("cause"));
            assertThat(auditMessage.toJsonString(), msgReqBodyField, hasKey("origin"));

            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.CLUSTER_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.ORIGIN), equalTo(AuditLog.Origin.REST.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_LAYER), equalTo(AuditLog.Origin.TRANSPORT.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_ID), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.UTC_TIMESTAMP), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.FORMAT_VERSION), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REMOTE_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_HOST_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_EFFECTIVE_USER), equalTo(USER.getName()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_EFFECTIVE_USER_AUTH_DOMAIN), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_HOST_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_VERSION), notNullValue());
        }
    }

    @Test
    public void testAuditLog_indexWriteHistory_createOperation_indexNameWithDateMath() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient(USER).trackResources()) {

            TestAuditlogImpl.clear();

            String indexPrefix = "test-index-date-math-name";
            DocNode settings = DocNode.of("settings", DocNode.of("index", DocNode.of("number_of_shards", 3, "number_of_replicas", 2)));
            String indexNameWithDateMath = "<" + indexPrefix + "{now{yyyy|UTC}}>";

            GenericRestClient.HttpResponse response = restClient.putJson("/" + URLEncoder.encode(indexNameWithDateMath, StandardCharsets.UTF_8.toString()), settings);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            AsyncAssert.awaitAssert("Messages arrived", () -> getMessagesByCategory(AuditMessage.Category.INDEX_WRITE).size() == 1, Duration.ofSeconds(2));
            DocNode auditMessage = DocNode.parse(Format.JSON).from(getMessagesByCategory(AuditMessage.Category.INDEX_WRITE).get(0).toJson());

            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.CATEGORY), equalTo(AuditMessage.Category.INDEX_WRITE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.COMPLIANCE_OPERATION), equalTo(AuditLog.Operation.CREATE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDICES), hasSize(1));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDICES).get(0), equalTo(indexNameWithDateMath));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.RESOLVED_INDICES), nullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_BODY), notNullValue());

            DocNode msgReqBodyField = DocNode.parse(Format.JSON).from(auditMessage.getAsString(AuditMessage.REQUEST_BODY));
            assertThat(auditMessage.toJsonString(), msgReqBodyField, hasKey("index"));
            assertThat(auditMessage.toJsonString(), msgReqBodyField, hasKey("settings"));
            assertThat(auditMessage.toJsonString(), msgReqBodyField, hasKey("mappings"));
            assertThat(auditMessage.toJsonString(), msgReqBodyField, hasKey("aliases"));
            assertThat(auditMessage.toJsonString(), msgReqBodyField, hasKey("cause"));
            assertThat(auditMessage.toJsonString(), msgReqBodyField, hasKey("origin"));

            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.CLUSTER_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.CLUSTER_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.ORIGIN), equalTo(AuditLog.Origin.REST.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_LAYER), equalTo(AuditLog.Origin.TRANSPORT.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_ID), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.UTC_TIMESTAMP), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.FORMAT_VERSION), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REMOTE_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_HOST_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_EFFECTIVE_USER), equalTo(USER.getName()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_EFFECTIVE_USER_AUTH_DOMAIN), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_HOST_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_VERSION), notNullValue());
        }
    }

    @Test
    public void testAuditLog_indexWriteHistory_createOperation_noLogWhenUserIsIgnored() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient(AUDIT_IGNORED_USER).trackResources()) {
            TestAuditlogImpl.clear();

            String indexName = "test-created-by-ignored-user";

            GenericRestClient.HttpResponse response = restClient.put("/" + indexName);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            Thread.sleep(1000);
            assertThat(TestAuditlogImpl.sb.toString(), getMessagesByCategory(AuditMessage.Category.INDEX_WRITE), hasSize(0));
        }
    }

    @Test
    public void testAuditLog_indexWriteHistory_deleteOperation() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient(USER).trackResources()) {
            String indexPrefix = "test-index-delete";
            String indexName = indexPrefix + "-1";
            String indexNameWithDateMath = "<" + indexPrefix + "{now{yyyy|UTC}}>";

            for(String index : Arrays.asList(indexName, indexNameWithDateMath)) {
                GenericRestClient.HttpResponse response = restClient.put("/" + URLEncoder.encode(index, StandardCharsets.UTF_8.toString()));
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            }

            TestAuditlogImpl.clear();

            String indexToRemove = indexPrefix + "*";

            GenericRestClient.HttpResponse response = restClient.delete("/" + indexToRemove);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            AsyncAssert.awaitAssert("Messages arrived", () -> getMessagesByCategory(AuditMessage.Category.INDEX_WRITE).size() == 1, Duration.ofSeconds(2));
            DocNode auditMessage = DocNode.parse(Format.JSON).from(getMessagesByCategory(AuditMessage.Category.INDEX_WRITE).get(0).toJson());

            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.CATEGORY), equalTo(AuditMessage.Category.INDEX_WRITE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.COMPLIANCE_OPERATION), equalTo(AuditLog.Operation.DELETE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDICES), hasSize(1));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDICES).get(0), equalTo(indexToRemove));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.RESOLVED_INDICES), nullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.CLUSTER_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.ORIGIN), equalTo(AuditLog.Origin.REST.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_LAYER), equalTo(AuditLog.Origin.TRANSPORT.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.UTC_TIMESTAMP), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.FORMAT_VERSION), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REMOTE_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_HOST_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_EFFECTIVE_USER), equalTo(USER.getName()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_EFFECTIVE_USER_AUTH_DOMAIN), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_VERSION), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_HOST_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_ID), notNullValue());
        }
    }

    @Test
    public void testAuditLog_indexWriteHistory_deleteOperation_noLogWhenUserIsIgnored() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient(AUDIT_IGNORED_USER).trackResources()) {
            String indexName = "test-deleted-by-ignored-user";

            GenericRestClient.HttpResponse response = restClient.put("/" + indexName);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            TestAuditlogImpl.clear();

            response = restClient.delete("/" + indexName);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            Thread.sleep(1000);
            assertThat(TestAuditlogImpl.sb.toString(), getMessagesByCategory(AuditMessage.Category.INDEX_WRITE), hasSize(0));
        }
    }

    @Test
    public void testAuditLog_indexSettingsWriteHistory() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient(USER).trackResources()) {

            String indexNamePrefix = "test-index-settings-update";
            String firstIndexName = indexNamePrefix + "-1";
            String secondIndexName = indexNamePrefix + "-2";
            String anotherIndex = "another-test-index-settings-update";
            int initialNumberOfReplicas = 2;

            DocNode settings = DocNode.of("settings", DocNode.of("index", DocNode.of("number_of_shards", 2, "number_of_replicas", initialNumberOfReplicas)));

            for (String name : Arrays.asList(firstIndexName, secondIndexName, anotherIndex)) {
                GenericRestClient.HttpResponse response = restClient.putJson("/" + name, settings);
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            }

            TestAuditlogImpl.clear();

            settings = DocNode.of("index", DocNode.of("number_of_replicas", initialNumberOfReplicas + 1));


            String indexNamePrefixWithWildcard = indexNamePrefix + "*";
            GenericRestClient.HttpResponse response = restClient.putJson("/" + indexNamePrefixWithWildcard + "/_settings", settings);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            AsyncAssert.awaitAssert("Messages arrived", () -> getMessagesByCategory(AuditMessage.Category.INDEX_WRITE).size() == 1, Duration.ofSeconds(2));

            DocNode auditMessage = DocNode.parse(Format.JSON).from(getMessagesByCategory(AuditMessage.Category.INDEX_WRITE).get(0).toJson());

            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.CATEGORY), equalTo(AuditMessage.Category.INDEX_WRITE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.COMPLIANCE_OPERATION), equalTo(AuditLog.Operation.UPDATE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDICES), hasSize(1));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDICES).get(0), equalTo(indexNamePrefixWithWildcard));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.RESOLVED_INDICES), nullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_BODY), notNullValue());

            DocNode msgReqBodyField = DocNode.parse(Format.JSON).from(auditMessage.getAsString(AuditMessage.REQUEST_BODY));
            assertThat(auditMessage.toJsonString(), msgReqBodyField, hasKey("indices"));
            assertThat(auditMessage.toJsonString(), msgReqBodyField, hasKey("settings"));
            assertThat(auditMessage.toJsonString(), msgReqBodyField, hasKey("preserve_existing"));
            assertThat(auditMessage.toJsonString(), msgReqBodyField, hasKey("origin"));

            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.CLUSTER_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.CLUSTER_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.ORIGIN), equalTo(AuditLog.Origin.REST.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_LAYER), equalTo(AuditLog.Origin.TRANSPORT.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_ID), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.UTC_TIMESTAMP), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.FORMAT_VERSION), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REMOTE_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_HOST_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_EFFECTIVE_USER), equalTo(USER.getName()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_EFFECTIVE_USER_AUTH_DOMAIN), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_HOST_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_VERSION), notNullValue());

            TestAuditlogImpl.clear();

            settings = DocNode.of("index", DocNode.of("number_of_replicas", initialNumberOfReplicas));


            response = restClient.putJson("/" + anotherIndex + "/_settings", settings);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            AsyncAssert.awaitAssert("Messages arrived", () -> getMessagesByCategory(AuditMessage.Category.INDEX_WRITE).size() == 1, Duration.ofSeconds(2));

            auditMessage = DocNode.parse(Format.JSON).from(getMessagesByCategory(AuditMessage.Category.INDEX_WRITE).get(0).toJson());

            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.CATEGORY), equalTo(AuditMessage.Category.INDEX_WRITE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.COMPLIANCE_OPERATION), equalTo(AuditLog.Operation.UPDATE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDICES), hasSize(1));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDICES).get(0), equalTo(anotherIndex));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.RESOLVED_INDICES), nullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_BODY), notNullValue());

            msgReqBodyField = DocNode.parse(Format.JSON).from(auditMessage.getAsString(AuditMessage.REQUEST_BODY));
            assertThat(auditMessage.toJsonString(), msgReqBodyField, hasKey("indices"));
            assertThat(auditMessage.toJsonString(), msgReqBodyField, hasKey("settings"));
            assertThat(auditMessage.toJsonString(), msgReqBodyField, hasKey("preserve_existing"));
            assertThat(auditMessage.toJsonString(), msgReqBodyField, hasKey("origin"));

            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.CLUSTER_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.CLUSTER_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.ORIGIN), equalTo(AuditLog.Origin.REST.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_LAYER), equalTo(AuditLog.Origin.TRANSPORT.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_ID), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.UTC_TIMESTAMP), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.FORMAT_VERSION), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REMOTE_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_HOST_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_EFFECTIVE_USER), equalTo(USER.getName()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_EFFECTIVE_USER_AUTH_DOMAIN), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_HOST_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_VERSION), notNullValue());
        }

    }

    @Test
    public void testAuditLog_indexSettingsWriteHistory_noLogWhenUserIsIgnored() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient(AUDIT_IGNORED_USER).trackResources()) {

            String indexName = "test-settings-update-by-ignored-user";
            int initialNumberOfReplicas = 2;

            DocNode settings = DocNode.of("settings", DocNode.of("index", DocNode.of("number_of_shards", 2, "number_of_replicas", initialNumberOfReplicas)));

            GenericRestClient.HttpResponse response = restClient.putJson("/" + indexName, settings);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            TestAuditlogImpl.clear();

            settings = DocNode.of("index", DocNode.of("number_of_replicas", initialNumberOfReplicas + 1));

            response = restClient.putJson("/" + indexName + "/_settings", settings);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            Thread.sleep(1000);
            assertThat(TestAuditlogImpl.sb.toString(), getMessagesByCategory(AuditMessage.Category.INDEX_WRITE), hasSize(0));
        }
    }

    @Test
    public void testAuditLog_indexSettingsWriteHistory_noLogWhenActionFails() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient(USER).trackResources()) {

            String indexName = "test-invalid-settings-update";
            int initialNumberOfReplicas = 2;

            DocNode settings = DocNode.of("settings", DocNode.of("index", DocNode.of("number_of_shards", 2, "number_of_replicas", initialNumberOfReplicas)));

            GenericRestClient.HttpResponse response = restClient.putJson("/" + indexName, settings);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            TestAuditlogImpl.clear();

            settings = DocNode.of("index", DocNode.of("number_of_shards", 3));

            response = restClient.putJson("/" + indexName + "/_settings", settings);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));

            Thread.sleep(1000);
            assertThat(TestAuditlogImpl.sb.toString(), getMessagesByCategory(AuditMessage.Category.INDEX_WRITE), hasSize(0));
        }
    }

    @Test
    public void testAuditLog_indexMappingsWriteHistory() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient(USER).trackResources()) {

            String indexNamePrefix = "test-index-mappings-update";
            String firstIndexName = indexNamePrefix + "-1";
            String secondIndexName = indexNamePrefix + "-2";
            String anotherIndex = "another-test-index-mappings-update";

            DocNode mappings = DocNode.of("mappings", DocNode.of("properties", DocNode.of("email", DocNode.of("type", "keyword"))));

            for (String name : Arrays.asList(firstIndexName, secondIndexName, anotherIndex)) {
                GenericRestClient.HttpResponse response = restClient.putJson("/" + name, mappings);
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            }

            TestAuditlogImpl.clear();

            mappings = DocNode.of("properties", DocNode.of("name", DocNode.of("type", "text")));


            String indexNamePrefixWithWildcard = indexNamePrefix + "*";
            GenericRestClient.HttpResponse response = restClient.putJson("/" + indexNamePrefixWithWildcard + "/_mappings", mappings);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            AsyncAssert.awaitAssert("Messages arrived", () -> getMessagesByCategory(AuditMessage.Category.INDEX_WRITE).size() == 1, Duration.ofSeconds(2));

            DocNode auditMessage = DocNode.parse(Format.JSON).from(getMessagesByCategory(AuditMessage.Category.INDEX_WRITE).get(0).toJson());

            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.CATEGORY), equalTo(AuditMessage.Category.INDEX_WRITE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.COMPLIANCE_OPERATION), equalTo(AuditLog.Operation.UPDATE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDICES), hasSize(1));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDICES).get(0), equalTo(indexNamePrefixWithWildcard));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.RESOLVED_INDICES), nullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_BODY), notNullValue());

            DocNode msgReqBodyField = DocNode.parse(Format.JSON).from(auditMessage.getAsString(AuditMessage.REQUEST_BODY));
            assertThat(auditMessage.toJsonString(), msgReqBodyField, hasKey("indices"));
            assertThat(auditMessage.toJsonString(), msgReqBodyField, hasKey("source"));
            assertThat(auditMessage.toJsonString(), msgReqBodyField, hasKey("write_index_only"));
            assertThat(auditMessage.toJsonString(), msgReqBodyField, hasKey("origin"));

            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.CLUSTER_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.CLUSTER_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.ORIGIN), equalTo(AuditLog.Origin.REST.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_LAYER), equalTo(AuditLog.Origin.TRANSPORT.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_ID), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.UTC_TIMESTAMP), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.FORMAT_VERSION), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REMOTE_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_HOST_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_EFFECTIVE_USER), equalTo(USER.getName()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_EFFECTIVE_USER_AUTH_DOMAIN), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_HOST_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_VERSION), notNullValue());

            TestAuditlogImpl.clear();

            mappings = DocNode.of("properties", DocNode.of("email", DocNode.of("type", "keyword")));


            response = restClient.putJson("/" + anotherIndex + "/_mappings", mappings);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            AsyncAssert.awaitAssert("Messages arrived", () -> getMessagesByCategory(AuditMessage.Category.INDEX_WRITE).size() == 1, Duration.ofSeconds(2));

            auditMessage = DocNode.parse(Format.JSON).from(getMessagesByCategory(AuditMessage.Category.INDEX_WRITE).get(0).toJson());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.CATEGORY), equalTo(AuditMessage.Category.INDEX_WRITE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.COMPLIANCE_OPERATION), equalTo(AuditLog.Operation.UPDATE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDICES), hasSize(1));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDICES).get(0), equalTo(anotherIndex));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.RESOLVED_INDICES), nullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_BODY), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.CLUSTER_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.ORIGIN), equalTo(AuditLog.Origin.REST.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_LAYER), equalTo(AuditLog.Origin.TRANSPORT.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_ID), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.UTC_TIMESTAMP), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.FORMAT_VERSION), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REMOTE_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_HOST_ADDRESS), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_EFFECTIVE_USER), equalTo(USER.getName()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.REQUEST_EFFECTIVE_USER_AUTH_DOMAIN), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_HOST_NAME), notNullValue());
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.NODE_VERSION), notNullValue());
        }

    }

    @Test
    public void testAuditLog_indexMappingsWriteHistory_noLogWhenUserIsIgnored() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient(AUDIT_IGNORED_USER).trackResources()) {

            String indexName = "test-mappings-update-by-ignored-user";

            DocNode mappings = DocNode.of("mappings", DocNode.of("properties", DocNode.of("email", DocNode.of("type", "keyword"))));

            GenericRestClient.HttpResponse response = restClient.putJson("/" + indexName, mappings);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            TestAuditlogImpl.clear();

            mappings = DocNode.of("properties", DocNode.of("name", DocNode.of("type", "text")));

            response = restClient.putJson("/" + indexName + "/_mappings", mappings);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            Thread.sleep(1000);
            assertThat(TestAuditlogImpl.sb.toString(), getMessagesByCategory(AuditMessage.Category.INDEX_WRITE), hasSize(0));
        }
    }

    @Test
    public void testAuditLog_indexMappingsWriteHistory_noLogWhenActionFails() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient(USER).trackResources()) {

            String indexName = "test-invalid-mappings-update";

            DocNode mappings = DocNode.of("mappings", DocNode.of("properties", DocNode.of("email", DocNode.of("type", "keyword"))));

            GenericRestClient.HttpResponse response = restClient.putJson("/" + indexName, mappings);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            TestAuditlogImpl.clear();

            mappings = DocNode.of("properties", DocNode.of("name", DocNode.of("type", "fake")));

            response = restClient.putJson("/" + indexName + "/_mappings", mappings);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));

            Thread.sleep(1000);
            assertThat(TestAuditlogImpl.sb.toString(), getMessagesByCategory(AuditMessage.Category.INDEX_WRITE), hasSize(0));
        }
    }

    private List<AuditMessage> getMessagesByCategory(AuditMessage.Category category) {
        return TestAuditlogImpl.messages.stream().filter(msg -> msg.getCategory() == category).collect(Collectors.toList());
    }
}
