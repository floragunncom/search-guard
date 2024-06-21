/*
 * Copyright 2023 floragunn GmbH
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

package com.floragunn.signals.proxy.rest;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.ClusterConfiguration;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.cluster.JvmEmbeddedEsCluster;
import com.floragunn.signals.Signals;
import com.floragunn.signals.SignalsModule;
import com.floragunn.signals.proxy.service.HttpProxyHostRegistry;
import com.floragunn.signals.proxy.service.ProxyCrudService;
import com.floragunn.signals.proxy.service.persistence.ProxyRepository;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.WatchBuilder;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.URLEncoder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsNullValue;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.docNodeSizeEqualTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class ProxyApiTest {

    private static final TestSgConfig.User ADMIN_USER = new TestSgConfig.User("admin").roles(TestSgConfig.Role.ALL_ACCESS
            .tenantPermission("SGS_GLOBAL_TENANT", "cluster:admin:searchguard:tenant:signals:*"));
    private static final String SIGNALS_PROXIES_INDEX_NAME = ".signals_proxies";

    private static List<HttpProxyHostRegistry> nodesHttpProxyHostRegistries;

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().clusterConfiguration(ClusterConfiguration.DEFAULT)
            .sslEnabled()//
            .user(ADMIN_USER)
            .enableModule(SignalsModule.class)//
            .nodeSettings("signals.enabled", true).waitForComponents("signals")
            .embedded().build();

    @BeforeClass
    public static void setup() {
        HttpProxyHostRegistry masterNodeRegistry = null;
        List<HttpProxyHostRegistry> dataNodesRegistries = new ArrayList<>();

        for (JvmEmbeddedEsCluster.Node node : cluster.nodes()) {
            Signals signals = node.getInjectable(Signals.class);
            assertThat(signals, notNullValue());
            if (node.esNode().isMasterEligible()) {
                masterNodeRegistry = signals.getHttpProxyHostRegistry();
            } else {
                dataNodesRegistries.add(signals.getHttpProxyHostRegistry());
            }
        }

        assertThat(masterNodeRegistry, notNullValue());
        assertThat(dataNodesRegistries, hasSize(2));
        assertThat(dataNodesRegistries, contains(notNullValue(), notNullValue()));

        nodesHttpProxyHostRegistries = ImmutableList.of(masterNodeRegistry).with(dataNodesRegistries);
    }

    @After
    public void removeProxies()  {
        try (Client client = cluster.getPrivilegedInternalNodeClient()){
            BulkByScrollResponse deleteResponse = client.execute(DeleteByQueryAction.INSTANCE, new DeleteByQueryRequest(SIGNALS_PROXIES_INDEX_NAME)
                    .setRefresh(true)
                    .setQuery(QueryBuilders.matchAllQuery())
            ).actionGet();
            assertThat(deleteResponse.getBulkFailures(), hasSize(0));
            nodesHttpProxyHostRegistries.forEach(HttpProxyHostRegistry::reloadAll);
        }
    }

    @Test
    public void putProxyConfig_shouldSaveNewConfigProxy() throws Exception {
        try (GenericRestClient adminCertClient = cluster.getAdminCertRestClient()) {
            String proxyId = "save-proxy";
            String proxyName = "save-proxy";
            String proxyUri = "http://localhost:8080";
            DocNode proxy = DocNode.of("uri", proxyUri, "name", proxyName);

            saveProxyWithProxyApi(proxyId, proxy);

            GenericRestClient.HttpResponse getResponse = adminCertClient.get("/" + SIGNALS_PROXIES_INDEX_NAME + "/_doc/" + proxyId);
            assertThat(getResponse.getBody(), getResponse.getStatusCode(), equalTo(HttpStatus.SC_OK));
            DocNode getResponseBody = getResponse.getBodyAsDocNode();
            assertThat(getResponseBody.toJsonString(), getResponseBody, containsValue("$.found", true));
            assertThat(getResponseBody.toJsonString(), getResponseBody, containsValue("$._id", proxyId));
            assertThat(getResponseBody.toJsonString(), getResponseBody, containsValue("$._source.name", proxyName));
            assertThat(getResponseBody.toJsonString(), getResponseBody, containsValue("$._source.uri", proxyUri));
        }
    }

    @Test
    public void putProxyConfig_shouldUpdateExistingProxyConfig() throws Exception {
        try (GenericRestClient adminCertClient = cluster.getAdminCertRestClient()) {
            String proxyId = "update-proxy";
            String proxyName = "update-proxy";
            String proxyUri = "http://localhost:8080";
            DocNode proxy = DocNode.of("uri", proxyUri, "name", proxyName);

            saveProxyWithProxyApi(proxyId, proxy);

            String updatedProxyUri = proxyUri.concat("123");
            String updatedName = "new-name";
            proxy = proxy.with("uri", updatedProxyUri).with("name", updatedName);

            saveProxyWithProxyApi(proxyId, proxy);

            GenericRestClient.HttpResponse getResponse = adminCertClient.get("/" + SIGNALS_PROXIES_INDEX_NAME + "/_doc/" + proxyId);
            assertThat(getResponse.getBody(), getResponse.getStatusCode(), equalTo(HttpStatus.SC_OK));
            DocNode getResponseBody = getResponse.getBodyAsDocNode();
            assertThat(getResponseBody.toJsonString(), getResponseBody, containsValue("$.found", true));
            assertThat(getResponseBody.toJsonString(), getResponseBody, containsValue("$._id", proxyId));
            assertThat(getResponseBody.toJsonString(), getResponseBody, containsValue("$._source.name", updatedName));
            assertThat(getResponseBody.toJsonString(), getResponseBody, containsValue("$._source.uri", updatedProxyUri));
        }
    }

    @Test
    public void putProxyConfig_shouldNotSaveProxyConfig_whenProxyIdContainsNotAllowedValue() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(ADMIN_USER)) {
            List<String> invalidIds = Arrays.asList("default", "NoNE", "http://127.0.0.1:123", "hTTps://127.0.0.1:343");

            for (String proxyId : invalidIds) {
                DocNode proxy = DocNode.of("uri", "127.0.0.1:3333", "name", "invalid-proxy");

                GenericRestClient.HttpResponse response = client.putJson("/_signals/proxies/" + URLEncoder.encode(proxyId, "UTF-8"), proxy);
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
                assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.error.details.id[0].error", "Invalid value"));
                assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.error.details.id[0].value", proxyId));
                assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.error.details.id[0].expected", "String not equal to any of: (default, none) and not starting with any of: (http:, https:)"));
            };
        }
    }

    @Test
    public void putProxyConfig_shouldNotSaveProxyConfig_whenAnyOfRequiredAttributesIsMissing() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(ADMIN_USER)) {
            String proxyId = "save-proxy";
            String proxyName = "";
            String proxyUri = "";
            DocNode proxy = DocNode.of("uri", proxyUri, "name", proxyName);

            GenericRestClient.HttpResponse response = client.putJson("/_signals/proxies/" + proxyId, proxy);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.error.details.uri[0].error", "Invalid value"));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.error.details.uri[0].value", proxyUri));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.error.details.uri[0].expected", "Valid URI"));

            proxyName = null;
            proxyUri = null;
            proxy = DocNode.of("uri", proxyUri, "name", proxyName);
            response = client.putJson("/_signals/proxies/" + proxyId, proxy);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.error.details.uri[0].error", "Invalid value"));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsNullValue("$.error.details.uri[0].value"));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.error.details.uri[0].expected", "Valid URI"));

            try (GenericRestClient adminCertClient = cluster.getAdminCertRestClient()) {
                GenericRestClient.HttpResponse getResponse = adminCertClient.get("/" + SIGNALS_PROXIES_INDEX_NAME + "/_doc/" + proxyId);
                assertThat(getResponse.getBody(), getResponse.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            }
        }
    }

    @Test
    public void putProxyConfig_shouldNotUpdateProxyConfig_whenAnyOfRequiredAttributesIsMissing() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(ADMIN_USER)) {
            String proxyId = "update-proxy";
            String initialProxyName = "name";
            String initialProxyUri = "http://localhost:9000";
            DocNode proxy = DocNode.of("uri", initialProxyUri, "name", initialProxyName);

            GenericRestClient.HttpResponse response = client.putJson("/_signals/proxies/" + proxyId, proxy);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            String updatedProxyName = "";
            String updatedProxyUri = "";
            proxy = proxy.with("uri", updatedProxyUri).with("name", updatedProxyName);
            response = client.putJson("/_signals/proxies/" + proxyId, proxy);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.error.details.uri[0].error", "Invalid value"));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.error.details.uri[0].value", updatedProxyUri));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.error.details.uri[0].expected", "Valid URI"));

            updatedProxyName = null;
            updatedProxyUri = null;
            proxy = DocNode.of("uri", updatedProxyUri).with("name", updatedProxyName);
            response = client.putJson("/_signals/proxies/" + proxyId, proxy);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.error.details.uri[0].error", "Invalid value"));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsNullValue("$.error.details.uri[0].value"));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.error.details.uri[0].expected", "Valid URI"));

            try (GenericRestClient adminCertClient = cluster.getAdminCertRestClient()) {
                GenericRestClient.HttpResponse getResponse = adminCertClient.get("/" + SIGNALS_PROXIES_INDEX_NAME + "/_doc/" + proxyId);
                assertThat(getResponse.getBody(), getResponse.getStatusCode(), equalTo(HttpStatus.SC_OK));
                DocNode getResponseBody = getResponse.getBodyAsDocNode();
                assertThat(getResponseBody.toJsonString(), getResponseBody, containsValue("$.found", true));
                assertThat(getResponseBody.toJsonString(), getResponseBody, containsValue("$._id", proxyId));
                assertThat(getResponseBody.toJsonString(), getResponseBody, containsValue("$._source.name", initialProxyName));
                assertThat(getResponseBody.toJsonString(), getResponseBody, containsValue("$._source.uri", initialProxyUri));
            }
        }
    }

    @Test
    public void getProxyConfigById_shouldReturnProxyConfig() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(ADMIN_USER)) {
            String firstProxyId = "save-proxy-1";
            DocNode firstProxy = DocNode.of("uri", "http://localhost:1", "name", "name-1");
            String secondProxyId = "save-proxy-2";
            DocNode secondProxy = DocNode.of("uri", "http://localhost:2", "name", "name-2");

            saveProxyWithProxyApi(firstProxyId, firstProxy);
            saveProxyWithProxyApi(secondProxyId, secondProxy);

            GenericRestClient.HttpResponse response = client.get("/_signals/proxies/" + firstProxyId);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            DocNode proxyFromResponse = response.getBodyAsDocNode();
            assertThat(proxyFromResponse.toJsonString(), proxyFromResponse, containsValue("$.data.id", firstProxyId));
            assertThat(proxyFromResponse.toJsonString(), proxyFromResponse, containsValue("$.data.name", firstProxy.get("name")));
            assertThat(proxyFromResponse.toJsonString(), proxyFromResponse, containsValue("$.data.uri", firstProxy.get("uri")));

            response = client.get("/_signals/proxies/" + secondProxyId);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            proxyFromResponse = response.getBodyAsDocNode();
            assertThat(proxyFromResponse.toJsonString(), proxyFromResponse, containsValue("$.data.id", secondProxyId));
            assertThat(proxyFromResponse.toJsonString(), proxyFromResponse, containsValue("$.data.name", secondProxy.get("name")));
            assertThat(proxyFromResponse.toJsonString(), proxyFromResponse, containsValue("$.data.uri", secondProxy.get("uri")));
        }
    }

    @Test
    public void getProxyConfig_shouldNotReturnProxyConfig_proxyWithGivenIdDoesNotExist() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(ADMIN_USER)) {
            String proxyId = "fake";
            GenericRestClient.HttpResponse response = client.get("/_signals/proxies/" + proxyId);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            String errorMessage = "Proxy with id '" + proxyId + "' not found.";
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.error.message", errorMessage));
        }
    }

    @Test
    public void deleteProxyConfig_shouldDeleteExistingProxyConfig() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(ADMIN_USER)) {
            String firstProxyId = "save-proxy-1";
            DocNode firstProxy = DocNode.of("uri", "http://localhost:1", "name", "name-11");
            String secondProxyId = "save-proxy-2";
            DocNode secondProxy = DocNode.of("uri", "http://localhost:2", "name", "name-22");

            saveProxyWithProxyApi(firstProxyId, firstProxy);
            saveProxyWithProxyApi(secondProxyId, secondProxy);

            GenericRestClient.HttpResponse response = client.get("/_signals/proxies/" + firstProxyId);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            response = client.get("/_signals/proxies/" + secondProxyId);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            response = client.delete("/_signals/proxies/" + firstProxyId);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            response = client.get("/_signals/proxies/" + firstProxyId);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            response = client.get("/_signals/proxies/" + secondProxyId);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
        }
    }

    @Test
    public void deleteProxyConfig_shouldNotDeleteProxyConfig_proxyWithGivenIdIsUsedByWatch() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(ADMIN_USER)) {
            String lowerCaseProxyId = "proxy-1".toLowerCase();
            String upperCaseProxyId = "proxy-1".toUpperCase();
            DocNode proxy = DocNode.of("uri", "http://localhost:1");
            String watchPath = "/_signals/watch/_main/webhook_with_proxy";

            saveProxyWithProxyApi(lowerCaseProxyId, proxy);
            saveProxyWithProxyApi(upperCaseProxyId, proxy);
            Watch watch = new WatchBuilder("test_with_stored_proxy").cronTrigger("0 0 */1 * * ?")
                    .then().postWebhook("http://localhost:3233").proxy(lowerCaseProxyId).name("webhook")
                    .build();

            GenericRestClient.HttpResponse response = client.putJson(watchPath, watch.toJson());
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_CREATED));

            response = client.delete("/_signals/proxies/" + lowerCaseProxyId);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_CONFLICT));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.error.message", "The proxy is still in use"));

            response = client.delete("/_signals/proxies/" + upperCaseProxyId);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            //remove watch
            response = client.delete(watchPath);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            response = client.delete("/_signals/proxies/" + lowerCaseProxyId);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
        }
    }

    @Test
    public void deleteProxyConfig_shouldNotDeleteProxyConfig_proxyWithGivenIdDoesNotExist() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(ADMIN_USER)) {
            String proxyId = "fake";

            GenericRestClient.HttpResponse response = client.delete("/_signals/proxies/" + proxyId);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
        }
    }

    @Test
    public void findAllProxiesConfigs_shouldReturnAllExistingProxies() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(ADMIN_USER)) {

            GenericRestClient.HttpResponse response = client.get("/_signals/proxies/");
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(response.getBody(), response.getBodyAsDocNode(), docNodeSizeEqualTo("$.data", 0));

            List<DocNode> proxies = saveRandomProxies(256);

            response = client.get("/_signals/proxies/");
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            DocNode responseBody = response.getBodyAsDocNode();
            assertThat(response.getBody(), responseBody, docNodeSizeEqualTo("$.data", proxies.size()));
            assertThat(response.getBody(), responseBody.getAsListOfNodes("data"), contains(proxies.toArray()));
        }
    }

    @Test
    public void findAllProxiesConfigs_shouldReturnAllExistingProxies_sortedByStoreTimeDesc() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(ADMIN_USER)) {
            Instant now = Instant.now();
            String firstProxyId = "proxy-1";
            DocNode firstProxy = DocNode.of("uri", "http://localhost:1", "name", "proxy-1","store_time", now.minusSeconds(100));
            String secondProxyId = "proxy-2";
            DocNode secondProxy = DocNode.of("uri", "http://localhost:2", "name", "proxy-2","store_time", now);
            String thirdProxyId = "proxy-3";
            DocNode thirdProxy = DocNode.of("uri", "http://localhost:3", "name", "proxy-3","store_time", now.plusSeconds(100));

            saveProxy(firstProxyId, firstProxy);
            saveProxy(secondProxyId, secondProxy);
            saveProxy(thirdProxyId, thirdProxy);

            GenericRestClient.HttpResponse response = client.get("/_signals/proxies/");
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(response.getBody(), response.getBodyAsDocNode(), docNodeSizeEqualTo("$.data", 3));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.data[0].id", thirdProxyId));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.data[1].id", secondProxyId));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.data[2].id", firstProxyId));
        }
    }

    @Test
    public void httpProxyHostRegistry_shouldBeNotifiedAboutProxyCreationUpdateAndDeletion() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(ADMIN_USER)) {
            String firstProxyId = "proxy-1";
            DocNode firstProxy = DocNode.of("uri", "http://localhost:1", "name", "proxy-1");
            String secondProxyId = "proxy-2";
            DocNode secondProxy = DocNode.of("uri", "http://localhost:2", "name", "proxy-2");

            Optional<HttpHost> firstProxyHost;
            Optional<HttpHost> secondProxyHost;

            //no proxy configured
            for (HttpProxyHostRegistry httpProxyHostRegistry : nodesHttpProxyHostRegistries) {
                firstProxyHost = httpProxyHostRegistry.findHttpProxyHost(firstProxyId);
                secondProxyHost = httpProxyHostRegistry.findHttpProxyHost(secondProxyId);
                assertThat(firstProxyHost.isPresent(), equalTo(false));
                assertThat(secondProxyHost.isPresent(), equalTo(false));
            }

            //add two proxies
            saveProxyWithProxyApi(firstProxyId, firstProxy);
            saveProxyWithProxyApi(secondProxyId, secondProxy);

            for (HttpProxyHostRegistry httpProxyHostRegistry : nodesHttpProxyHostRegistries) {
                firstProxyHost = httpProxyHostRegistry.findHttpProxyHost(firstProxyId);
                secondProxyHost = httpProxyHostRegistry.findHttpProxyHost(secondProxyId);
                assertThat(firstProxyHost.isPresent(), equalTo(true));
                assertThat(secondProxyHost.isPresent(), equalTo(true));
                assertThat(firstProxyHost.get().toURI(), equalTo(firstProxy.getAsString("uri")));
                assertThat(secondProxyHost.get().toURI(), equalTo(secondProxy.getAsString("uri")));
            }

            //update second proxy
            secondProxy = secondProxy.with("uri", "http://new-uri:123");
            saveProxyWithProxyApi(secondProxyId, secondProxy);

            for (HttpProxyHostRegistry httpProxyHostRegistry : nodesHttpProxyHostRegistries) {
                firstProxyHost = httpProxyHostRegistry.findHttpProxyHost(firstProxyId);
                secondProxyHost = httpProxyHostRegistry.findHttpProxyHost(secondProxyId);
                assertThat(firstProxyHost.isPresent(), equalTo(true));
                assertThat(secondProxyHost.isPresent(), equalTo(true));
                assertThat(firstProxyHost.get().toURI(), equalTo(firstProxy.getAsString("uri")));
                assertThat(secondProxyHost.get().toURI(), equalTo(secondProxy.getAsString("uri")));
            }

            //remove first proxy
            GenericRestClient.HttpResponse response = client.delete("/_signals/proxies/" + firstProxyId);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            for (HttpProxyHostRegistry httpProxyHostRegistry : nodesHttpProxyHostRegistries) {
                firstProxyHost = httpProxyHostRegistry.findHttpProxyHost(firstProxyId);
                secondProxyHost = httpProxyHostRegistry.findHttpProxyHost(secondProxyId);
                assertThat(firstProxyHost.isPresent(), equalTo(false));
                assertThat(secondProxyHost.isPresent(), equalTo(true));
            }
        }
    }

    @Test
    public void httpProxyHostRegistry_shouldLoadAllProxiesOnStartup() throws Exception {
        String firstProxyId = "proxy-1";
        DocNode firstProxy = DocNode.of("uri", "http://localhost:1", "name", "proxy-1");
        String secondProxyId = "proxy-2";
        DocNode secondProxy = DocNode.of("uri", "http://localhost:2", "name", "proxy-2");

        saveProxyWithProxyApi(firstProxyId, firstProxy);
        saveProxyWithProxyApi(secondProxyId, secondProxy);

        PrivilegedConfigClient client = PrivilegedConfigClient.adapt(cluster.getInternalNodeClient());
        Signals signals = cluster.getInjectable(Signals.class);
        assertThat(signals, notNullValue());
        ProxyCrudService proxyCrudService = new ProxyCrudService(new ProxyRepository(signals.getSignalsSettings(), client));
        HttpProxyHostRegistry httpProxyHostRegistry = new HttpProxyHostRegistry(proxyCrudService);

        httpProxyHostRegistry.reloadAll();

        Optional<HttpHost> firstProxyHost = httpProxyHostRegistry.findHttpProxyHost(firstProxyId);
        Optional<HttpHost> secondProxyHost = httpProxyHostRegistry.findHttpProxyHost(secondProxyId);
        assertThat(firstProxyHost.isPresent(), equalTo(true));
        assertThat(secondProxyHost.isPresent(), equalTo(true));
    }

    @Test
    public void shouldNotAccessProxiesIndexDirectly() throws Exception {
        try(GenericRestClient client = cluster.getRestClient(ADMIN_USER);
            GenericRestClient adminCertClient = cluster.getAdminCertRestClient()) {
            String firstProxyId = "proxy-1";
            DocNode firstProxy = DocNode.of("uri", "http://localhost:1", "name", "proxy-1");
            String secondProxyId = "proxy-2";
            DocNode secondProxy = DocNode.of("uri", "http://localhost:2", "name", "proxy-2");

            saveProxyWithProxyApi(firstProxyId, firstProxy);
            saveProxyWithProxyApi(secondProxyId, secondProxy);

            GenericRestClient.HttpResponse response = client.get("/_signals/proxies");
            assertThat(response.getStatusCode(), equalTo(200));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("data", 2));

            response = client.get("/" + SIGNALS_PROXIES_INDEX_NAME + "/_search");
            assertThat(response.getStatusCode(), equalTo(403));

            response = adminCertClient.get("/" + SIGNALS_PROXIES_INDEX_NAME + "/_search");
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("hits.total.value", 2));
        }
    }

    private void saveProxyWithProxyApi(String proxyId, DocNode proxy) throws Exception {
        try (GenericRestClient client = cluster.getRestClient(ADMIN_USER)) {
            GenericRestClient.HttpResponse response = client.putJson("/_signals/proxies/" + proxyId, proxy);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
        }
    }

    private List<DocNode> saveRandomProxies(int noOfProxiesToSave) {
        BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        List<DocNode> proxies = new ArrayList<>();
        IntStream.range(0, noOfProxiesToSave).forEach(proxyNo -> {
            String id = "proxy-" + proxyNo;
            String name = "name-" + proxyNo;
            DocNode proxy = DocNode.of("uri", "http://localhost:" + proxyNo, "name", name, "store_time", Instant.now().minusSeconds(proxyNo));
            bulkRequest.add(new IndexRequest(SIGNALS_PROXIES_INDEX_NAME).id(id).source(proxy));
            proxies.add(proxy.with("id", id).without("store_time"));
        });
        try (Client client = cluster.getInternalNodeClient()) {
            BulkResponse bulkResponse = client.bulk(bulkRequest).actionGet();
            assertThat(bulkResponse.hasFailures(), equalTo(false));
            assertThat(bulkResponse.status().getStatus(), equalTo(200));
        }
        return proxies;
    }

    private void saveProxy(String id, DocNode proxy) {
        try (Client client = cluster.getInternalNodeClient()) {
            IndexRequest indexRequest = new IndexRequest(SIGNALS_PROXIES_INDEX_NAME)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).id(id).source(proxy);
            IndexResponse response = client.index(indexRequest).actionGet();
            assertThat(response.getResult(), anyOf(
                    equalTo(DocWriteResponse.Result.CREATED),
                    equalTo(DocWriteResponse.Result.UPDATED)
            ));
        }
    }
}
