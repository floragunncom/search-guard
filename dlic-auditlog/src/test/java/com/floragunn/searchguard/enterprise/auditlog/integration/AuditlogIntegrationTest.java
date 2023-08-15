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

import java.time.Duration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AuditlogIntegrationTest {

    static final TestSgConfig.User USER = new TestSgConfig.User("user")
            .roles(new TestSgConfig.Role("all_access").indexPermissions("*").on("*").clusterPermissions("*"));

    static TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(
            new TestSgConfig.Authc.Domain("basic/internal_users_db"));

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder()
            .users(USER)
            .authc(AUTHC)
            .sslEnabled()
            .enterpriseModulesEnabled()
            .nodeSettings(
                ConfigConstants.SEARCHGUARD_AUDIT_TYPE_DEFAULT, TestAuditlogImpl.class.getName(),
                ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, true,
                ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_REST, true,
                ConfigConstants.SEARCHGUARD_AUDIT_THREADPOOL_SIZE, 0
            )
            .build();

    @Test
    public void auditLogForKibanaLogin() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient()) {
            TestAuditlogImpl.clear();

            DocNode requestBody = DocNode.of("user", USER.getName(), "password", USER.getPassword());
            GenericRestClient.HttpResponse response = restClient.postJson("/_searchguard/auth/session", requestBody);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_CREATED));

            AsyncAssert.awaitAssert("Messages arrived", () -> TestAuditlogImpl.messages.size() == 1, Duration.ofSeconds(2));
            DocNode auditMessage = DocNode.parse(Format.JSON).from(TestAuditlogImpl.sb.toString());

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

            AsyncAssert.awaitAssert("Messages arrived", () -> TestAuditlogImpl.messages.size() == 1, Duration.ofSeconds(2));
            auditMessage = DocNode.parse(Format.JSON).from(TestAuditlogImpl.sb.toString());

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
    public void auditLogForKibanaLogout() throws Exception {

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

            AsyncAssert.awaitAssert("Messages arrived", () -> TestAuditlogImpl.messages.size() == 1, Duration.ofSeconds(2));
            DocNode auditMessage = DocNode.parse(Format.JSON).from(TestAuditlogImpl.sb.toString());

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
}
