/*
 * Copyright 2020-2023 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.floragunn.signals.truststore.rest;

import static com.floragunn.searchguard.test.TestSgConfig.Role.ALL_ACCESS;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.docNodeSizeEqualTo;
import static com.floragunn.signals.truststore.rest.TruststoreLoader.CERT_1_ISSUER;
import static com.floragunn.signals.truststore.rest.TruststoreLoader.CERT_1_SUBJECT;
import static com.floragunn.signals.truststore.rest.TruststoreLoader.CERT_2_ISSUER;
import static com.floragunn.signals.truststore.rest.TruststoreLoader.CERT_2_SUBJECT;
import static com.floragunn.signals.truststore.rest.TruststoreLoader.CERT_3_ISSUER;
import static com.floragunn.signals.truststore.rest.TruststoreLoader.CERT_3_SUBJECT;
import static com.floragunn.signals.truststore.rest.TruststoreLoader.CERT_4_ISSUER;
import static com.floragunn.signals.truststore.rest.TruststoreLoader.CERT_4_SUBJECT;
import static com.floragunn.signals.truststore.rest.TruststoreLoader.NAME_TRUST_STORE;
import static com.floragunn.signals.truststore.rest.TruststoreLoader.storeTruststore;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.TestSgConfig.User;
import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.signals.SignalsModule;
import com.floragunn.signals.settings.SignalsSettings.SignalsStaticSettings.IndexNames;

public class TrustedCertificatesRestActionHandlersAndTruststoreIndexMappingTest {

    private static final Logger log = LogManager.getLogger(TrustedCertificatesRestActionHandlersAndTruststoreIndexMappingTest.class);
    public static final String TRUSTSTORE_ID_1 = "truststore-id-001";
    public static final String TRUSTSTORE_ID_2 = "truststore-id-002";
    public static final String TRUSTSTORE_ID_3 = "truststore-id-003";

    private static final User USER_ADMIN = new User("admin").roles(ALL_ACCESS);
    private static final User READONLY_USER = new User("readonly_user")//
        .roles(new Role("read-only-role").indexPermissions("SGS_READ").on("*"));

    private final static Role READ_TRUSTSTORES_ROLE = new Role("read-truststores-role").clusterPermissions(
        "cluster:admin:searchguard:signals:truststores/findall",
        "cluster:admin:searchguard:signals:truststores/findone");

    private final static User READ_TRUSTSTORES_USER = new User("read-truststores-user").roles(READ_TRUSTSTORES_ROLE);

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled()//
        .user(USER_ADMIN).user(READONLY_USER).user(READ_TRUSTSTORES_USER).enableModule(SignalsModule.class)//
        .nodeSettings("signals.enabled", true)
        .build();

    @After
    public void clearData() {
        Client client = cluster.getPrivilegedInternalNodeClient();
        SearchResponse searchResponse = client.search(new SearchRequest(IndexNames.TRUSTSTORES)).actionGet();
        for(SearchHit hit : searchResponse.getHits().getHits()) {
            String id = hit.getId();
            client.delete(new DeleteRequest(IndexNames.TRUSTSTORES).id(id).setRefreshPolicy(IMMEDIATE)).actionGet();
            log.info("Document with id '{}' deleted from index '{}'.", id, IndexNames.TRUSTSTORES);
        }
    }

    @Test
    public void shouldNotFindTrustStoreWhichDoesNotExist() throws Exception {
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {

            HttpResponse response = client.get("/_signals/truststores/does-not-exists");

            log.info("Load one truststore by id response '{}'", response.getBody());
            assertThat(response.getStatusCode(), equalTo(404));
            assertThat(response.getBody(), not(isEmptyOrNullString()));
        }
    }

    @Test
    public void shouldLoadTruststoreById() throws Exception {
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            storeTruststore(client, TRUSTSTORE_ID_1, TruststoreLoader.PEM_THREE_CERTIFICATES);
            storeTruststore(client, TRUSTSTORE_ID_2, TruststoreLoader.PEM_TWO_CERTIFICATES);
            storeTruststore(client, TRUSTSTORE_ID_3, TruststoreLoader.PEM_ONE_CERTIFICATES);

            HttpResponse response = client.get("/_signals/truststores/" + TRUSTSTORE_ID_1);

            log.info("Load one truststore by id response '{}'", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.certificates", 3));
            assertThat(body,  containsValue("data.certificates[0].issuer", CERT_1_ISSUER));
            assertThat(body,  containsValue("data.certificates[0].subject", CERT_1_SUBJECT));
            assertThat(body,  containsValue("data.certificates[1].issuer", CERT_2_ISSUER));
            assertThat(body,  containsValue("data.certificates[1].subject", CERT_2_SUBJECT));
            assertThat(body,  containsValue("data.certificates[2].issuer", CERT_3_ISSUER));
            assertThat(body,  containsValue("data.certificates[2].subject", CERT_3_SUBJECT));
        }
    }

    @Test
    public void shouldLoadExactlyTheSamePemFile() throws Exception {
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            String genuinePem = storeTruststore(client, TRUSTSTORE_ID_2, TruststoreLoader.PEM_THREE_CERTIFICATES);

            HttpResponse response = client.get("/_signals/truststores/" + TRUSTSTORE_ID_2);

            log.info("Load one truststore by id response '{}'", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            String returnedPem = body.getAsNode("data").getAsString("raw_pem");
            assertThat(returnedPem, equalTo(genuinePem));
        }
    }

    @Test
    public void shouldGetEmptyTruststoreList() throws Exception {
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {

            HttpResponse response = client.get("/_signals/truststores");

            log.info("Get truststore by id '{}'", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data", 0));
        }
    }

    @Test
    public void shouldGetListWithOneTruststore() throws Exception {
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            storeTruststore(client, TRUSTSTORE_ID_1, TruststoreLoader.PEM_THREE_CERTIFICATES);

            HttpResponse response = client.get("/_signals/truststores");

            log.info("Get all truststores response '{}'", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data", 1));
            assertThat(body, docNodeSizeEqualTo("data[0].certificates", 3));
            assertThat(body,  containsValue("data[0].certificates[0].issuer", CERT_1_ISSUER));
            assertThat(body,  containsValue("data[0].certificates[0].subject", CERT_1_SUBJECT));
            assertThat(body,  containsValue("data[0].certificates[1].issuer", CERT_2_ISSUER));
            assertThat(body,  containsValue("data[0].certificates[1].subject", CERT_2_SUBJECT));
            assertThat(body,  containsValue("data[0].certificates[2].issuer", CERT_3_ISSUER));
            assertThat(body,  containsValue("data[0].certificates[2].subject", CERT_3_SUBJECT));
        }
    }

    @Test
    public void shouldGetListWithMultipleTruststores() throws Exception {
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            storeTruststore(client, TRUSTSTORE_ID_1, TruststoreLoader.PEM_ONE_CERTIFICATES);
            storeTruststore(client, TRUSTSTORE_ID_2, TruststoreLoader.PEM_TWO_CERTIFICATES);
            storeTruststore(client, TRUSTSTORE_ID_3, TruststoreLoader.PEM_THREE_CERTIFICATES);

            HttpResponse response = client.get("/_signals/truststores");

            log.info("Get all truststores response '{}'", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data", 3));
            assertThat(body,  containsValue("data[0].id", TRUSTSTORE_ID_1));
            assertThat(body,  docNodeSizeEqualTo("data[0].certificates", 1));
            assertThat(body,  containsValue("data[0].certificates[0].issuer", CERT_1_ISSUER));
            assertThat(body,  containsValue("data[0].certificates[0].subject", CERT_1_SUBJECT));
            assertThat(body,  containsValue("data[1].id", TRUSTSTORE_ID_2));
            assertThat(body,  docNodeSizeEqualTo("data[1].certificates", 2));
            assertThat(body,  containsValue("data[1].certificates[0].issuer", CERT_4_ISSUER));
            assertThat(body,  containsValue("data[1].certificates[0].subject", CERT_4_SUBJECT));
            assertThat(body,  containsValue("data[1].certificates[1].issuer", CERT_2_ISSUER));
            assertThat(body,  containsValue("data[1].certificates[1].subject", CERT_2_ISSUER));
            assertThat(body,  containsValue("data[2].id", TRUSTSTORE_ID_3));
            assertThat(body,  docNodeSizeEqualTo("data[2].certificates", 3));
            assertThat(body,  containsValue("data[2].certificates[0].issuer", CERT_1_ISSUER));
            assertThat(body,  containsValue("data[2].certificates[0].subject", CERT_1_SUBJECT));
            assertThat(body,  containsValue("data[2].certificates[1].issuer", CERT_2_ISSUER));
            assertThat(body,  containsValue("data[2].certificates[1].subject", CERT_2_SUBJECT));
            assertThat(body,  containsValue("data[2].certificates[2].issuer", CERT_3_ISSUER));
            assertThat(body,  containsValue("data[2].certificates[2].subject", CERT_3_SUBJECT));
        }
    }

    @Test
    public void shouldUploadTruststore() throws Exception {
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            String pem = TruststoreLoader.loadCertificates(TruststoreLoader.PEM_THREE_CERTIFICATES);
            String body = DocNode.of("name", NAME_TRUST_STORE, "pem", pem).toJsonString();

            HttpResponse response = client.putJson("/_signals/truststores/" + TRUSTSTORE_ID_1, body);

            log.info("Upload truststore status code '{}' and response body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode bodyAsDocNode = response.getBodyAsDocNode();
            assertThat(bodyAsDocNode, containsValue("data.name", NAME_TRUST_STORE));
            assertThat(bodyAsDocNode, containsValue("data.id", TRUSTSTORE_ID_1));
            assertThat(bodyAsDocNode, docNodeSizeEqualTo("data.certificates", 3));
            assertThat(bodyAsDocNode,  containsValue("data.certificates[0].issuer", CERT_1_ISSUER));
            assertThat(bodyAsDocNode,  containsValue("data.certificates[0].subject", CERT_1_SUBJECT));
            assertThat(bodyAsDocNode,  containsValue("data.certificates[1].issuer", CERT_2_ISSUER));
            assertThat(bodyAsDocNode,  containsValue("data.certificates[1].subject", CERT_2_SUBJECT));
            assertThat(bodyAsDocNode,  containsValue("data.certificates[2].issuer", CERT_3_ISSUER));
            assertThat(bodyAsDocNode,  containsValue("data.certificates[2].subject", CERT_3_SUBJECT));
        }
    }

    @Test
    public void shouldReplaceTruststore() throws Exception {
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            storeTruststore(client, TRUSTSTORE_ID_1, TruststoreLoader.PEM_THREE_CERTIFICATES);
            String pem = TruststoreLoader.loadCertificates(TruststoreLoader.PEM_ONE_CERTIFICATES);
            String body = DocNode.of("name", NAME_TRUST_STORE, "pem", pem).toJsonString();

            HttpResponse response = client.putJson("/_signals/truststores/" + TRUSTSTORE_ID_1, body);

            log.info("Replace truststore status code '{}' and response body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode bodyAsDocNode = response.getBodyAsDocNode();
            assertThat(bodyAsDocNode, containsValue("data.name", NAME_TRUST_STORE));
            assertThat(bodyAsDocNode, containsValue("data.id", TRUSTSTORE_ID_1));
            assertThat(bodyAsDocNode, docNodeSizeEqualTo("data.certificates", 1));
            assertThat(bodyAsDocNode, containsValue("data.raw_pem", pem));
        }
    }

    @Test
    public void shouldUploadExpiredCertificate() throws Exception {
        Date validityStartDate = Date.from(LocalDateTime.of(1999, 1, 1, 12, 0)//
            .atZone(ZoneOffset.UTC)//
            .toInstant());
        Date validityEndDate = Date.from(LocalDateTime.of(2001, 1, 1, 12, 0)//
            .atZone(ZoneOffset.UTC)//
            .toInstant());
        String expiredCertificatePem = TestCertificates.builder()//
            .ca("CN=root.ca.example.com,OU=SearchGuard,O=SearchGuard", validityStartDate, validityEndDate)//
            .build().getCaCertificate().getCertificateString();
        final String name = "I am a little outdated";
        String body = DocNode.of("name", name, "pem", expiredCertificatePem).toJsonString();
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {

            HttpResponse response = client.putJson("/_signals/truststores/" + TRUSTSTORE_ID_1, body);

            log.info("Upload truststore status code '{}' and response body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode responseBody = response.getBodyAsDocNode();
            assertThat(responseBody, containsValue("data.name", name));
            assertThat(responseBody,  containsValue("data.certificates[0].not_before", "1999-01-01T12:00:00.000Z"));
            assertThat(responseBody,  containsValue("data.certificates[0].not_after", "2001-01-01T12:00:00.000Z"));
        }
    }

    @Test
    public void shouldValidateCertificatesAndReportErrors() throws Exception {
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            String invalidCertificatePem = "-----BEGIN CERTIFICATE-----invalid certificate-----END CERTIFICATE-----";
            String body = DocNode.of("name", NAME_TRUST_STORE, "pem", invalidCertificatePem).toJsonString();

            HttpResponse response = client.putJson("/_signals/truststores/" + TRUSTSTORE_ID_1, body);

            log.info("Replace truststore status code '{}' and response body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(400));
        }
    }

    @Test
    public void shouldReturnErrorsOccuredDuringCertificateParsing() throws Exception {
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            String body = DocNode.of("name", NAME_TRUST_STORE, "pem", "this is invalid certificate").toJsonString();

            HttpResponse response = client.putJson("/_signals/truststores/" + TRUSTSTORE_ID_1, body);

            log.info("Replace truststore status code '{}' and response body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(400));
        }
    }

    @Test
    public void shouldNotDeleteTruststoreIfTruststoreDoesNotExists() throws Exception {
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {

            HttpResponse response = client.delete("/_signals/truststores/not-exists");

            assertThat(response.getStatusCode(), equalTo(404));
            assertThat(response.getBody(), not(isEmptyOrNullString()));
        }
    }

    @Test
    public void shouldDeleteExistingTruststore() throws Exception {
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            storeTruststore(client, TRUSTSTORE_ID_1, TruststoreLoader.PEM_THREE_CERTIFICATES);
            storeTruststore(client, TRUSTSTORE_ID_2, TruststoreLoader.PEM_TWO_CERTIFICATES);
            storeTruststore(client, TRUSTSTORE_ID_3, TruststoreLoader.PEM_ONE_CERTIFICATES);

            HttpResponse response = client.delete("/_signals/truststores/" + TRUSTSTORE_ID_1);

            assertThat(response.getStatusCode(), equalTo(200));
            assertThat(client.get("/_signals/truststores/" + TRUSTSTORE_ID_1).getStatusCode(), equalTo(404));
            assertThat(client.get("/_signals/truststores/" + TRUSTSTORE_ID_2).getStatusCode(), equalTo(200));
            assertThat(client.get("/_signals/truststores/" + TRUSTSTORE_ID_3).getStatusCode(), equalTo(200));
        }
    }

    @Test
    public void shouldDeleteAnotherExistingTruststore() throws Exception {
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            storeTruststore(client, TRUSTSTORE_ID_1, TruststoreLoader.PEM_THREE_CERTIFICATES);
            storeTruststore(client, TRUSTSTORE_ID_2, TruststoreLoader.PEM_TWO_CERTIFICATES);
            storeTruststore(client, TRUSTSTORE_ID_3, TruststoreLoader.PEM_ONE_CERTIFICATES);

            HttpResponse response = client.delete("/_signals/truststores/" + TRUSTSTORE_ID_2);

            assertThat(response.getStatusCode(), equalTo(200));
            assertThat(client.get("/_signals/truststores/" + TRUSTSTORE_ID_1).getStatusCode(), equalTo(200));
            assertThat(client.get("/_signals/truststores/" + TRUSTSTORE_ID_2).getStatusCode(), equalTo(404));
            assertThat(client.get("/_signals/truststores/" + TRUSTSTORE_ID_3).getStatusCode(), equalTo(200));
        }
    }

    @Test
    public void shouldNotAccessTruststoreIndexDirectly() throws Exception {
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            storeTruststore(client, TRUSTSTORE_ID_1, TruststoreLoader.PEM_THREE_CERTIFICATES);
            storeTruststore(client, TRUSTSTORE_ID_2, TruststoreLoader.PEM_TWO_CERTIFICATES);
            storeTruststore(client, TRUSTSTORE_ID_3, TruststoreLoader.PEM_ONE_CERTIFICATES);
            HttpResponse response = client.get("/_signals/truststores");
            assertThat(response.getStatusCode(), equalTo(200));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("data", 3));

            response = client.get("/" + IndexNames.TRUSTSTORES + "/_search");
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("hits.total.value", 0));
        }
    }

    @Test
    public void shouldNotGetCertificatesWhenUserHasNoRequiredPermission() throws Exception {
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            storeTruststore(client, TRUSTSTORE_ID_1, TruststoreLoader.PEM_THREE_CERTIFICATES);
            storeTruststore(client, TRUSTSTORE_ID_2, TruststoreLoader.PEM_TWO_CERTIFICATES);
            storeTruststore(client, TRUSTSTORE_ID_3, TruststoreLoader.PEM_ONE_CERTIFICATES);
        }
        try(GenericRestClient client = cluster.getRestClient(READONLY_USER)) {
            HttpResponse response = client.get("/_signals/truststores");
            assertThat(response.getStatusCode(), equalTo(403));
        }
    }

    @Test
    public void shouldReadOneTruststoreWithReadTruststoresUserAccount() throws Exception {
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            storeTruststore(client, TRUSTSTORE_ID_1, TruststoreLoader.PEM_THREE_CERTIFICATES);
        }
        try(GenericRestClient client = cluster.getRestClient(READ_TRUSTSTORES_USER)) {
            HttpResponse response = client.get("/_signals/truststores/" + TRUSTSTORE_ID_1);
            assertThat(response.getStatusCode(), equalTo(200));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("data.certificates", 3));
        }
    }

    @Test
    public void shouldReadAllTruststoreWithReadTruststoresUserAccount() throws Exception {
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            storeTruststore(client, TRUSTSTORE_ID_1, TruststoreLoader.PEM_THREE_CERTIFICATES);
        }
        try(GenericRestClient client = cluster.getRestClient(READ_TRUSTSTORES_USER)) {
            HttpResponse response = client.get("/_signals/truststores");
            assertThat(response.getStatusCode(), equalTo(200));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("data[0].certificates", 3));
        }
    }

    @Test
    public void shouldNotCreateTruststoresWithReadTruststoresUserAccount() throws Exception {
        try(GenericRestClient client = cluster.getRestClient(READ_TRUSTSTORES_USER)) {
            String pem = TruststoreLoader.loadCertificates(TruststoreLoader.PEM_THREE_CERTIFICATES);
            String body = DocNode.of("name", NAME_TRUST_STORE, "pem", pem).toJsonString();

            HttpResponse response = client.putJson("/_signals/truststores/" + TRUSTSTORE_ID_1, body);

            assertThat(response.getStatusCode(), equalTo(403));
        }
    }

    @Test
    public void shouldUsePredefinedMappingForTruststoreIndex() throws Exception {
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            /*
                The value of the name contain a date, therefore ElasticSearch would generate mapping for date field.
                Therefore, this test will work only when mapping for name field is predefined and contain text type.
             */
            String nameWhichLooksLikeDate = "2023-05-08T13:35:53.508955Z";
            storeTruststore(client, "certificate-id-1", TruststoreLoader.PEM_ONE_CERTIFICATES, nameWhichLooksLikeDate);
            String pem = TruststoreLoader.loadCertificates(TruststoreLoader.PEM_THREE_CERTIFICATES);
            String body = DocNode.of("name", "regular and valid name", "pem", pem).toJsonString();

            GenericRestClient.HttpResponse response = client.putJson("/_signals/truststores/" + "another-id", body);

            log.info("Upload truststore status code '{}' and response body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
        }
    }
}