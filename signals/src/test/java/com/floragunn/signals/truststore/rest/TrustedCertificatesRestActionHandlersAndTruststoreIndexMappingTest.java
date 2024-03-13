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

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.TestSgConfig.User;
import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.signals.SignalsModule;
import com.floragunn.signals.settings.SignalsSettings.SignalsStaticSettings.IndexNames;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.WatchBuilder;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;

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
import static com.floragunn.signals.truststore.rest.TruststoreLoader.PEM_ONE_CERTIFICATES;
import static com.floragunn.signals.truststore.rest.TruststoreLoader.PEM_THREE_CERTIFICATES;
import static com.floragunn.signals.truststore.rest.TruststoreLoader.PEM_TWO_CERTIFICATES;
import static com.floragunn.signals.truststore.rest.TruststoreLoader.storeTruststore;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class TrustedCertificatesRestActionHandlersAndTruststoreIndexMappingTest {

    private static final Logger log = LogManager.getLogger(TrustedCertificatesRestActionHandlersAndTruststoreIndexMappingTest.class);
    public static final String TRUSTSTORE_ID_1 = "truststore-id-001";
    public static final String TRUSTSTORE_ID_2 = "truststore-id-002";
    public static final String TRUSTSTORE_ID_3 = "truststore-id-003";

    private static final User USER_ADMIN = new User("admin").roles(ALL_ACCESS
            .tenantPermission("SGS_GLOBAL_TENANT", "cluster:admin:searchguard:tenant:signals:*"));
    private static final User READONLY_USER = new User("readonly_user")//
        .roles(new Role("read-only-role").indexPermissions("SGS_READ").on("*"));

    private final static Role READ_TRUSTSTORES_ROLE = new Role("read-truststores-role").clusterPermissions(
        "cluster:admin:searchguard:signals:truststores/findall",
        "cluster:admin:searchguard:signals:truststores/findone");

    private final static User READ_TRUSTSTORES_USER = new User("read-truststores-user").roles(READ_TRUSTSTORES_ROLE);

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().singleNode().sslEnabled()//
        .user(USER_ADMIN).user(READONLY_USER).user(READ_TRUSTSTORES_USER).enableModule(SignalsModule.class)//
        .nodeSettings("signals.enabled", true).waitForComponents("signals")
        .embedded().build();

    @After
    public void clearData() {
        try(Client client = cluster.getPrivilegedInternalNodeClient()) {
            SearchResponse searchResponse = client.search(new SearchRequest(IndexNames.TRUSTSTORES)).actionGet();
            for(SearchHit hit : searchResponse.getHits().getHits()) {
                String id = hit.getId();
                client.delete(new DeleteRequest(IndexNames.TRUSTSTORES).id(id).setRefreshPolicy(IMMEDIATE)).actionGet();
                log.info("Document with id '{}' deleted from index '{}'.", id, IndexNames.TRUSTSTORES);
            }
        }
    }

    @Test
    public void shouldNotFindTrustStoreWhichDoesNotExist() throws Exception {
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {

            HttpResponse response = client.get("/_signals/truststores/does-not-exists");

            log.info("Load one truststore by id response '{}'", response.getBody());
            assertThat(response.getStatusCode(), equalTo(404));
            assertThat(response.getBody(), not(emptyOrNullString()));
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

            List<DocNode> truststores = saveRandomTruststores(12);

            HttpResponse response = client.get("/_signals/truststores");

            log.info("Get all truststores response '{}'", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            List<DocNode> responseData = response.getBodyAsDocNode().getAsListOfNodes("data");
            assertThat(responseData, hasSize(truststores.size()));
            for (int i = 0; i < truststores.size(); i++) {
                assertThat(responseData.get(i), containsValue("id", truststores.get(i).get("id")));

                List<DocNode> truststoreCerts = truststores.get(i).getAsListOfNodes("certificates");
                assertThat(responseData.get(i), docNodeSizeEqualTo("certificates", truststoreCerts.size()));

                for (int j = 0; j < truststoreCerts.size(); j++) {
                    assertThat(responseData.get(i), containsValue("certificates[" + j + "].issuer", truststoreCerts.get(j).get("issuer")));
                    assertThat(responseData.get(i), containsValue("certificates[" + j + "].subject", truststoreCerts.get(j).get("subject")));
                }
            }
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
            assertThat(response.getBody(), not(emptyOrNullString()));
        }
    }

    @Test
    public void shouldNotDeleteTruststoreIfTruststoreIsUsedByWatch() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            String watchPath = "/_signals/watch/_main/webhook_with_truststore";

            String lowerCaseTruststoreId = TRUSTSTORE_ID_1.toLowerCase();
            String upperCaseTruststoreId = TRUSTSTORE_ID_1.toUpperCase();
            storeTruststore(client, lowerCaseTruststoreId, TruststoreLoader.PEM_THREE_CERTIFICATES);
            storeTruststore(client, upperCaseTruststoreId, TruststoreLoader.PEM_THREE_CERTIFICATES);

            Watch watch = new WatchBuilder("test_with_truststore").cronTrigger("0 0 */1 * * ?")
                    .then().postWebhook("http://localhost:3233").truststoreId(lowerCaseTruststoreId).name("webhook")
                    .build();

            GenericRestClient.HttpResponse response = client.putJson(watchPath, watch.toJson());
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_CREATED));

            response = client.delete("/_signals/truststores/" + lowerCaseTruststoreId);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_CONFLICT));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.error.message", "The truststore is still in use"));

            response = client.delete("/_signals/truststores/" + upperCaseTruststoreId);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            //remove watch
            response = client.delete(watchPath);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            response = client.delete("/_signals/truststores/" + lowerCaseTruststoreId);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
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
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN);
            GenericRestClient adminCertClient = cluster.getAdminCertRestClient()) {
            storeTruststore(client, TRUSTSTORE_ID_1, TruststoreLoader.PEM_THREE_CERTIFICATES);
            storeTruststore(client, TRUSTSTORE_ID_2, TruststoreLoader.PEM_TWO_CERTIFICATES);
            storeTruststore(client, TRUSTSTORE_ID_3, TruststoreLoader.PEM_ONE_CERTIFICATES);
            HttpResponse response = client.get("/_signals/truststores");
            assertThat(response.getStatusCode(), equalTo(200));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("data", 3));

            response = client.get("/" + IndexNames.TRUSTSTORES + "/_search");
            assertThat(response.getStatusCode(), equalTo(403));

            response = adminCertClient.get("/" + IndexNames.TRUSTSTORES + "/_search");
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("hits.total.value", 3));
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

    private List<DocNode> saveRandomTruststores(int noOfTruststores) throws Exception {
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            Map<Integer, DocNode> certNoToRepresentation = ImmutableMap.of(
                    1, DocNode.array(DocNode.of("issuer", CERT_1_ISSUER, "subject", CERT_1_SUBJECT)),
                    2, DocNode.array(
                            DocNode.of("issuer", CERT_4_ISSUER, "subject", CERT_4_SUBJECT),
                            DocNode.of("issuer", CERT_2_ISSUER, "subject", CERT_2_ISSUER)
                    ),
                    3, DocNode.array(
                            DocNode.of("issuer", CERT_1_ISSUER, "subject", CERT_1_SUBJECT),
                            DocNode.of("issuer", CERT_2_ISSUER, "subject", CERT_2_SUBJECT),
                            DocNode.of("issuer", CERT_3_ISSUER, "subject", CERT_3_SUBJECT)
                    )
            );
            Map<Integer, String> certNoToPem = ImmutableMap.of(
                    1, PEM_ONE_CERTIFICATES,
                    2, PEM_TWO_CERTIFICATES,
                    3, PEM_THREE_CERTIFICATES
            );
            List<DocNode> trustStores = new ArrayList<>();
            for (int trustNo = 1; trustNo <= noOfTruststores; trustNo++) {
                int certNo = new Random().nextInt(3) + 1;
                String trustName = "name-" + trustNo;
                storeTruststore(client, String.valueOf(trustNo), certNoToPem.get(certNo), trustName);
                trustStores.add(DocNode.of("id", trustNo, "name", trustName, "certificates", certNoToRepresentation.get(certNo)));
            }
            return trustStores;
        }
    }
}