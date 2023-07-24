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

package com.floragunn.searchguard.enterprise.auditlog.compliance;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.enterprise.auditlog.impl.AuditMessage;
import com.floragunn.searchguard.enterprise.auditlog.integration.TestAuditlogImpl;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchsupport.junit.AsyncAssert;
import org.apache.http.HttpStatus;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class ComplianceAuditlogNewFrameworkTest {

    static final TestSgConfig.User COMPLIANCE_WRITE_HISTORY_IGNORED_USER = new TestSgConfig.User("compliance_write_history_ignored")
            .roles(new TestSgConfig.Role("all_access").indexPermissions("*").on("*").clusterPermissions("*"));
    static final TestSgConfig.User USER = new TestSgConfig.User("user")
            .roles(new TestSgConfig.Role("all_access").indexPermissions("*").on("*").clusterPermissions("*"));

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder()
            .users(COMPLIANCE_WRITE_HISTORY_IGNORED_USER, USER)
            .sslEnabled()
            .enterpriseModulesEnabled()
            .nodeSettings(
                ConfigConstants.SEARCHGUARD_AUDIT_TYPE_DEFAULT, TestAuditlogImpl.class.getName(),
                ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, false,
                ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_REST, false,
                ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_WRITE_LOG_DIFFS, true,
                ConfigConstants.SEARCHGUARD_AUDIT_THREADPOOL_SIZE, 0,
                ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_WRITE_IGNORE_USERS, Collections.singletonList(COMPLIANCE_WRITE_HISTORY_IGNORED_USER.getName())
            )
            .build();

    @Test
    public void testComposableIndexTemplateWriteHistory() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient(USER).trackResources()) {
            TestAuditlogImpl.clear();

            String templateName = "test-composable-template";
            DocNode template = DocNode.of("index_patterns", Collections.singletonList("test-composable"));

            GenericRestClient.HttpResponse response = restClient.putJson("/_index_template/" + templateName, template);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            AsyncAssert.awaitAssert("Messages arrived", () -> TestAuditlogImpl.messages.size() == 1, Duration.ofSeconds(2));
            DocNode auditMessage = DocNode.parse(Format.JSON).from(TestAuditlogImpl.sb.toString());

            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.CATEGORY), equalTo(AuditMessage.Category.COMPLIANCE_INDEX_TEMPLATE_WRITE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.COMPLIANCE_OPERATION), equalTo(AuditLog.Operation.CREATE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDEX_TEMPLATES), hasSize(1));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDEX_TEMPLATES).get(0), equalTo(templateName));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.RESOLVED_INDEX_TEMPLATES), hasSize(1));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.RESOLVED_INDEX_TEMPLATES).get(0), equalTo(templateName));

            TestAuditlogImpl.clear();

            template = template.with("index_patterns", Collections.singletonList("composable-new-pattern"));

            response = restClient.putJson("/_index_template/" + templateName, template);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            AsyncAssert.awaitAssert("Messages arrived", () -> TestAuditlogImpl.messages.size() == 1, Duration.ofSeconds(2));
            auditMessage = DocNode.parse(Format.JSON).from(TestAuditlogImpl.sb.toString());

            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.CATEGORY), equalTo(AuditMessage.Category.COMPLIANCE_INDEX_TEMPLATE_WRITE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.COMPLIANCE_OPERATION), equalTo(AuditLog.Operation.UPDATE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDEX_TEMPLATES), hasSize(1));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDEX_TEMPLATES).get(0), equalTo(templateName));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.RESOLVED_INDEX_TEMPLATES), hasSize(1));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.RESOLVED_INDEX_TEMPLATES).get(0), equalTo(templateName));
            assertThat(auditMessage.toJsonString(), auditMessage.getBoolean(AuditMessage.COMPLIANCE_DIFF_IS_NOOP), is(false));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.COMPLIANCE_DIFF_CONTENT), containsString("op\":\"replace"));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.COMPLIANCE_DIFF_CONTENT), containsString("path\":\"/index_patterns"));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.COMPLIANCE_DIFF_CONTENT), containsString("value\":[\"composable-new-pattern\"]"));

            TestAuditlogImpl.clear();

            response = restClient.delete("/_index_template/" + templateName);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            AsyncAssert.awaitAssert("Messages arrived", () -> TestAuditlogImpl.messages.size() == 1, Duration.ofSeconds(2));
            auditMessage = DocNode.parse(Format.JSON).from(TestAuditlogImpl.sb.toString());
            
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.CATEGORY), equalTo(AuditMessage.Category.COMPLIANCE_INDEX_TEMPLATE_WRITE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.COMPLIANCE_OPERATION), equalTo(AuditLog.Operation.DELETE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDEX_TEMPLATES), hasSize(1));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDEX_TEMPLATES).get(0), equalTo(templateName));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.RESOLVED_INDEX_TEMPLATES), hasSize(1));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.RESOLVED_INDEX_TEMPLATES).get(0), equalTo(templateName));
        }
    }

    @Test
    public void testLegacyIndexTemplateWriteHistory() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient(USER).trackResources()){
            TestAuditlogImpl.clear();

            String templateName = "test-legacy-template";
            DocNode template = DocNode.of("index_patterns", Collections.singletonList("test-legacy"));

            GenericRestClient.HttpResponse response = restClient.putJson("/_template/" + templateName, template);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            AsyncAssert.awaitAssert("Messages arrived", () -> TestAuditlogImpl.messages.size() == 1, Duration.ofSeconds(2));
            DocNode auditMessage = DocNode.parse(Format.JSON).from(TestAuditlogImpl.sb.toString());

            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.CATEGORY), equalTo(AuditMessage.Category.COMPLIANCE_INDEX_TEMPLATE_WRITE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.COMPLIANCE_OPERATION), equalTo(AuditLog.Operation.CREATE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDEX_TEMPLATES), hasSize(1));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDEX_TEMPLATES).get(0), equalTo(templateName));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.RESOLVED_INDEX_TEMPLATES), hasSize(1));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.RESOLVED_INDEX_TEMPLATES).get(0), equalTo(templateName));

            TestAuditlogImpl.clear();

            template = template.with("index_patterns", Collections.singletonList("legacy-new-pattern"));

            response = restClient.putJson("/_template/" + templateName, template);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            AsyncAssert.awaitAssert("Messages arrived", () -> TestAuditlogImpl.messages.size() == 1, Duration.ofSeconds(2));
            auditMessage = DocNode.parse(Format.JSON).from(TestAuditlogImpl.sb.toString());

            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.CATEGORY), equalTo(AuditMessage.Category.COMPLIANCE_INDEX_TEMPLATE_WRITE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.COMPLIANCE_OPERATION), equalTo(AuditLog.Operation.UPDATE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDEX_TEMPLATES), hasSize(1));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDEX_TEMPLATES).get(0), equalTo(templateName));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.RESOLVED_INDEX_TEMPLATES), hasSize(1));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.RESOLVED_INDEX_TEMPLATES).get(0), equalTo(templateName));
            assertThat(auditMessage.toJsonString(), auditMessage.getBoolean(AuditMessage.COMPLIANCE_DIFF_IS_NOOP), is(false));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.COMPLIANCE_DIFF_CONTENT), containsString("op\":\"replace"));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.COMPLIANCE_DIFF_CONTENT), containsString("path\":\"/test-legacy-template/index_patterns"));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsString(AuditMessage.COMPLIANCE_DIFF_CONTENT), containsString("value\":[\"legacy-new-pattern\"]"));

            TestAuditlogImpl.clear();

            response = restClient.delete("/_template/" + templateName);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            AsyncAssert.awaitAssert("Messages arrived", () -> TestAuditlogImpl.messages.size() == 1, Duration.ofSeconds(2));
            auditMessage = DocNode.parse(Format.JSON).from(TestAuditlogImpl.sb.toString());

            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.CATEGORY), equalTo(AuditMessage.Category.COMPLIANCE_INDEX_TEMPLATE_WRITE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.get(AuditMessage.COMPLIANCE_OPERATION), equalTo(AuditLog.Operation.DELETE.name()));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDEX_TEMPLATES), hasSize(1));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.INDEX_TEMPLATES).get(0), equalTo(templateName));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.RESOLVED_INDEX_TEMPLATES), hasSize(1));
            assertThat(auditMessage.toJsonString(), auditMessage.getAsListOfStrings(AuditMessage.RESOLVED_INDEX_TEMPLATES).get(0), equalTo(templateName));
        }
    }

    @Test
    public void testComposableIndexTemplateWriteHistory_noLogWhenActionFails() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient(USER).trackResources()) {
            TestAuditlogImpl.clear();

            String templateName = "test-composable-template";
            DocNode template = DocNode.of("index_patterns", Collections.singletonList("test-composable"), "template", DocNode.of("settings", DocNode.of("number_of_shards", 0)));

            GenericRestClient.HttpResponse response = restClient.putJson("/_index_template/" + templateName, template);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));

            response = restClient.delete("/_index_template/" + 1 + templateName);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));

            Thread.sleep(1000);
            assertThat(TestAuditlogImpl.messages, hasSize(0));
        }
    }

    @Test
    public void testLegacyIndexTemplateWriteHistory_noLogWhenActionFails() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient(USER).trackResources()) {
            TestAuditlogImpl.clear();

            String templateName = "test-legacy-template";
            DocNode template = DocNode.of("index_patterns", Collections.singletonList("test-legacy"), "settings", DocNode.of("number_of_shards", 0));

            GenericRestClient.HttpResponse response = restClient.putJson("/_template/" + templateName, template);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));

            response = restClient.delete("/_template/" + 1 + templateName);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));

            Thread.sleep(1000);
            assertThat(TestAuditlogImpl.messages, hasSize(0));
        }
    }

    @Test
    public void testComposableIndexTemplateWriteHistory_noLogWhenUserIsIgnored() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient(COMPLIANCE_WRITE_HISTORY_IGNORED_USER).trackResources()) {
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
            assertThat(TestAuditlogImpl.messages, hasSize(0));
        }
    }

    @Test
    public void testLegacyIndexTemplateWriteHistory_noLogWhenUserIsIgnored() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient(COMPLIANCE_WRITE_HISTORY_IGNORED_USER).trackResources()) {
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
            assertThat(TestAuditlogImpl.messages, hasSize(0));
        }
    }

}
