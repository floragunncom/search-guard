/*
 * Copyright 2025 floragunn GmbH
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

package com.floragunn.signals.actions.watch.execute;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.diag.DiagnosticContext;
import com.floragunn.searchsupport.proxy.wiremock.WireMockRequestHeaderAddingFilter;
import com.floragunn.searchsupport.util.EsLogging;
import com.floragunn.signals.MockWebserviceProvider;
import com.floragunn.signals.Signals;
import com.floragunn.signals.SignalsTenant;
import com.floragunn.signals.accounts.AccountRegistry;
import com.floragunn.signals.execution.SimulationMode;
import com.floragunn.signals.proxy.service.HttpProxyHostRegistry;
import com.floragunn.signals.script.SignalsScriptContextFactory;
import com.floragunn.signals.settings.SignalsSettings;
import com.floragunn.signals.truststore.service.TrustManagerRegistry;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.WatchBuilder;
import com.floragunn.signals.watch.action.handlers.email.EmailAccount;
import com.floragunn.signals.watch.checks.Condition;
import com.floragunn.signals.watch.severity.SeverityLevel;
import com.floragunn.signals.watch.severity.SeverityMapping;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.http.HttpHost;
import org.apache.lucene.search.TotalHits;
import org.awaitility.Awaitility;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsFieldPointedByJsonPath;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsNullValue;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsOnlyFields;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.docNodeSizeEqualTo;
import static com.floragunn.signals.CertificatesParser.parseCertificates;
import static com.floragunn.signals.CertificatesParser.toTruststore;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TransportExecuteWatchActionTest {

    private static final String STORED_TRUSTSTORE_ID = "stored-truststore-id";
    private static final String STORED_PROXY_ID = "stored-proxy-id";
    private static final String TENANT = "_main";

    private static final WireMockRequestHeaderAddingFilter REQUEST_HEADER_ADDING_FILTER = new WireMockRequestHeaderAddingFilter("Proxy", "wire-mock");

    @Rule
    public WireMockRule wireMockProxy = new WireMockRule(WireMockConfiguration.options()
            .bindAddress("127.0.0.8")
            .enableBrowserProxying(true)
            .proxyPassThrough(true)
            .dynamicPort()
            .extensions(REQUEST_HEADER_ADDING_FILTER));

    @ClassRule
    public static EsLogging esLogging = new EsLogging();

    @Mock
    public Signals signals;

    @Mock
    public TransportService transportService;

    @Mock
    public ThreadPool threadPool;

    @Mock
    public ActionFilters actionFilters;

    @Mock
    public ScriptService scriptService;

    @Mock
    public Client client;

    @Mock
    public Settings settings;

    @Mock
    public ClusterService clusterService;

    @Mock
    public DiagnosticContext diagnosticContext;

    @Mock
    public Task task;

    @Mock
    private ActionListener<ExecuteWatchResponse> listener;

    @Mock
    public SignalsTenant signalsTenant;

    @Mock
    public AccountRegistry accountRegistry;

    @Mock
    public TrustManagerRegistry trustManagerRegistry;

    @Mock
    public HttpProxyHostRegistry httpProxyHostRegistry;

    @Mock
    public SeverityMapping.SeverityValueScript.Factory severityValueScriptFactory;

    @Mock
    public Condition.ConditionScript.Factory conditionScriptFactory;

    @Mock
    public SeverityMapping.SeverityValueScript severityValueScript;

    @Mock
    public Condition.ConditionScript conditionScript;

    @Captor
    private ArgumentCaptor<ExecuteWatchResponse> executeResponseCaptor;

    public NamedXContentRegistry xContentRegistry;

    private TransportExecuteWatchAction executeWatchAction;

    @Before
    public void setUp() throws Exception {
        xContentRegistry = new NamedXContentRegistry(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putTransient("_sg_user", User.forUser("test-user").requestedTenant(TENANT).build());
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(threadPool.generic()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(signals.getAccountRegistry()).thenReturn(accountRegistry);
        when(signals.getTruststoreRegistry()).thenReturn(trustManagerRegistry);
        when(signals.getHttpProxyHostRegistry()).thenReturn(httpProxyHostRegistry);
        when(signals.getSignalsSettings()).thenReturn(new SignalsSettings(Settings.EMPTY));
        when(signals.getTenant(any(User.class))).thenReturn(signalsTenant);
        when(signalsTenant.getName()).thenReturn(TENANT);
        executeWatchAction = new TransportExecuteWatchAction(signals, transportService, threadPool, actionFilters, scriptService, xContentRegistry, client, settings, clusterService, diagnosticContext);
    }

    @Test
    public void testExecuteAnonymousWatchWhichUsesStoredTruststore() throws Exception {
        try (MockWebserviceProvider webhookProvider = new MockWebserviceProvider.Builder("/hook").ssl(true).clientAuth(false).build()) {
            X509ExtendedTrustManager trustManager = trustManagerFromPem(webhookProvider.trustedCertificatePem("root-ca"));
            when(trustManagerRegistry.findTrustManager(eq(STORED_TRUSTSTORE_ID))).thenReturn(Optional.ofNullable(trustManager));
            Watch watch = new WatchBuilder("tls-webhook-execute-test").atMsInterval(100)
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then()
                    .postWebhook(webhookProvider.getUri()).truststoreId(STORED_TRUSTSTORE_ID).throttledFor("0")
                    .name("testhook").build();

            executeWatchAction.doExecute(task, new ExecuteWatchRequest(null, watch.toJson(), false, SimulationMode.FOR_REAL, false), listener);

            Awaitility.await()
                    .atMost(Duration.ofSeconds(2))
                    .until(() -> webhookProvider.getRequestCount() > 0);

            verify(listener, never()).onFailure(any());
            verify(listener).onResponse(executeResponseCaptor.capture());
            ExecuteWatchResponse response = executeResponseCaptor.getValue();
            assertThat(response.getStatus(), equalTo(ExecuteWatchResponse.Status.EXECUTED));
            DocNode result = DocNode.parse(Format.JSON).from(response.getResult().utf8ToString());
            assertThat(result.toJsonString(), result, containsValue("$.status.code", "ACTION_EXECUTED"));
            assertThat(result.toJsonString(), result, containsValue("$.status.detail", "All actions have been executed"));
            assertThat(result.toJsonString(), result, docNodeSizeEqualTo("$.actions", 1));
            assertThat(result.toJsonString(), result, containsValue("$.actions[0].name", "testhook"));
            assertThat(result.toJsonString(), result, containsValue("$.actions[0].status.code", "ACTION_EXECUTED"));
        }
    }

    @Test
    public void testExecuteAnonymousWatch() throws Exception {
        mockTemplateScript(
                new Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, "{\"query\":{\"match_all\":{}}}", Collections.emptyMap()),
                SignalsScriptContextFactory.TEMPLATE_CONTEXT,
                "{\"query\":{\"match_all\":{}}}"
        );
        PlainActionFuture<SearchResponse> searchResponseFuture = new PlainActionFuture<>();
        searchResponseFuture.onResponse(searchResponseWithOneHit());
        ArgumentCaptor<SearchRequest> searchRequestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
        when(client.search(searchRequestCaptor.capture())).thenReturn(searchResponseFuture);

        PlainActionFuture<DocWriteResponse> docWriteResponseFuture = new PlainActionFuture<>();
        docWriteResponseFuture.onResponse(indexResponse("testsink"));
        ArgumentCaptor<IndexRequest> indexRequestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        when(client.index(indexRequestCaptor.capture())).thenReturn(docWriteResponseFuture);

        Watch watch = new WatchBuilder("execution_test_anon").cronTrigger("*/2 * * * * ?").search("testsource").query("{\"match_all\" : {} }")
                .as("testsearch").put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink").name("testsink").build();

        executeWatchAction.doExecute(task, new ExecuteWatchRequest(null, watch.toJson(), false, SimulationMode.FOR_REAL, false), listener);

        assertThat(searchRequestCaptor.getValue().indices(), arrayWithSize(1));
        assertThat(searchRequestCaptor.getValue().indices(), arrayContainingInAnyOrder("testsource"));

        IndexRequest indexRequest = indexRequestCaptor.getValue();
        DocNode indexRequestSource = DocNode.parse(Format.JSON).from(indexRequest.source().utf8ToString());
        assertThat(indexRequest.index(), equalTo("testsink"));
        assertThat(indexRequestSource.toString(), indexRequestSource, containsOnlyFields("$", "testsearch", "teststatic"));
        assertThat(indexRequestSource.toString(), indexRequestSource, containsValue("$.testsearch.hits.hits[0]._source.a", "b"));
        assertThat(indexRequestSource.toString(), indexRequestSource, containsValue("$.teststatic.bla.blub", "42"));

        verify(listener, never()).onFailure(any());
        verify(listener).onResponse(executeResponseCaptor.capture());
        ExecuteWatchResponse response = executeResponseCaptor.getValue();
        assertThat(response.getStatus(), equalTo(ExecuteWatchResponse.Status.EXECUTED));
        DocNode result = DocNode.parse(Format.JSON).from(response.getResult().utf8ToString());
        assertThat(result.toJsonString(), result, containsValue("$.status.code", "ACTION_EXECUTED"));
        assertThat(result.toJsonString(), result, containsValue("$.status.detail", "All actions have been executed"));
        assertThat(result.toJsonString(), result, docNodeSizeEqualTo("$.actions", 1));
        assertThat(result.toJsonString(), result, containsValue("$.actions[0].name", "testsink"));
        assertThat(result.toJsonString(), result, containsValue("$.actions[0].status.code", "ACTION_EXECUTED"));
    }

    @Test
    public void testExecuteWatchById() throws Exception {
        mockTemplateScript(
                new Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, "{\"query\":{\"match_all\":{}}}", Collections.emptyMap()),
                SignalsScriptContextFactory.TEMPLATE_CONTEXT,
                        "{\"query\":{\"match_all\":{}}}"
        );
        when(signalsTenant.getConfigIndexName()).thenReturn(".signals-watch");
        when(signalsTenant.getWatchIdForConfigIndex("execute-by-id")).thenReturn("execute-by-id");
        Watch watch = new WatchBuilder("execute-by-id").cronTrigger("0 0 */1 * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink").name("testsink").build();

        GetRequestBuilder getRequestBuilder = spy(new GetRequestBuilder(client));
        when(client.prepareGet()).thenReturn(getRequestBuilder);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(0);
            listener.onResponse(getWatchResponse(watch.toJson()));
            return null;
        }).when(getRequestBuilder).execute(any());

        PlainActionFuture<SearchResponse> searchResponseFuture = new PlainActionFuture<>();
        searchResponseFuture.onResponse(searchResponseWithOneHit());
        ArgumentCaptor<SearchRequest> searchRequestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
        when(client.search(searchRequestCaptor.capture())).thenReturn(searchResponseFuture);

        PlainActionFuture<DocWriteResponse> docWriteResponseFuture = new PlainActionFuture<>();
        docWriteResponseFuture.onResponse(indexResponse("testsink"));
        ArgumentCaptor<IndexRequest> indexRequestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        when(client.index(indexRequestCaptor.capture())).thenReturn(docWriteResponseFuture);

        ExecuteWatchRequest executeWatchRequest = new ExecuteWatchRequest(
                "execute-by-id", null, false, SimulationMode.FOR_REAL, false
        );
        executeWatchAction.doExecute(task, executeWatchRequest, listener);

        verify(getRequestBuilder).setId(eq("execute-by-id"));
        verify(getRequestBuilder).setIndex(eq(".signals-watch"));

        assertThat(searchRequestCaptor.getValue().indices(), arrayWithSize(1));
        assertThat(searchRequestCaptor.getValue().indices(), arrayContainingInAnyOrder("testsource"));

        IndexRequest indexRequest = indexRequestCaptor.getValue();
        DocNode indexRequestSource = DocNode.parse(Format.JSON).from(indexRequest.source().utf8ToString());
        assertThat(indexRequest.index(), equalTo("testsink"));
        assertThat(indexRequestSource.toString(), indexRequestSource, containsOnlyFields("$", "testsearch", "teststatic"));
        assertThat(indexRequestSource.toString(), indexRequestSource, containsValue("$.testsearch.hits.hits[0]._source.a", "b"));
        assertThat(indexRequestSource.toString(), indexRequestSource, containsValue("$.teststatic.bla.blub", "42"));

        verify(listener, never()).onFailure(any());
        verify(listener).onResponse(executeResponseCaptor.capture());
        ExecuteWatchResponse response = executeResponseCaptor.getValue();
        assertThat(response.getStatus(), equalTo(ExecuteWatchResponse.Status.EXECUTED));
        DocNode result = DocNode.parse(Format.JSON).from(response.getResult().utf8ToString());
        assertThat(result.toJsonString(), result, containsValue("$.status.code", "ACTION_EXECUTED"));
        assertThat(result.toJsonString(), result, containsValue("$.status.detail", "All actions have been executed"));
        assertThat(result.toJsonString(), result, docNodeSizeEqualTo("$.actions", 1));
        assertThat(result.toJsonString(), result, containsValue("$.actions[0].name", "testsink"));
        assertThat(result.toJsonString(), result, containsValue("$.actions[0].status.code", "ACTION_EXECUTED"));
    }

    @Test
    public void testExecuteWatchByIdWhichUsesUploadedTruststore() throws Exception {
        try (MockWebserviceProvider webhookProvider = new MockWebserviceProvider.Builder("/hook").ssl(true).clientAuth(false).build()) {
            X509ExtendedTrustManager trustManager = trustManagerFromPem(webhookProvider.trustedCertificatePem("root-ca"));
            when(trustManagerRegistry.findTrustManager(eq(STORED_TRUSTSTORE_ID))).thenReturn(Optional.ofNullable(trustManager));
            when(signalsTenant.getConfigIndexName()).thenReturn(".signals-watch");
            when(signalsTenant.getWatchIdForConfigIndex("execute-by-id-use-stored-truststore")).thenReturn("execute-by-id-use-stored-truststore");
            mockTemplateScript(
                    new Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, "{\"query\":{\"match_all\":{}}}", Collections.emptyMap()),
                    SignalsScriptContextFactory.TEMPLATE_CONTEXT,
                    "{\"query\":{\"match_all\":{}}}"
            );
            Watch watch = new WatchBuilder("execute-by-id-use-stored-truststore").cronTrigger("0 0 */1 * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then()
                    .postWebhook(webhookProvider.getUri()).truststoreId(STORED_TRUSTSTORE_ID).throttledFor("0").name("send-http-request")
                    .build();

            GetRequestBuilder getRequestBuilder = spy(new GetRequestBuilder(client));
            when(client.prepareGet()).thenReturn(getRequestBuilder);
            doAnswer(invocation -> {
                ActionListener<GetResponse> listener = invocation.getArgument(0);
                listener.onResponse(getWatchResponse(watch.toJson()));
                return null;
            }).when(getRequestBuilder).execute(any());

            PlainActionFuture<SearchResponse> searchResponseFuture = new PlainActionFuture<>();
            searchResponseFuture.onResponse(searchResponseWithOneHit());
            ArgumentCaptor<SearchRequest> searchRequestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
            when(client.search(searchRequestCaptor.capture())).thenReturn(searchResponseFuture);

            ExecuteWatchRequest executeWatchRequest = new ExecuteWatchRequest(
                    "execute-by-id-use-stored-truststore", null, false,
                    SimulationMode.FOR_REAL, false
            );
            executeWatchAction.doExecute(task, executeWatchRequest, listener);

            verify(getRequestBuilder).setId(eq("execute-by-id-use-stored-truststore"));
            verify(getRequestBuilder).setIndex(eq(".signals-watch"));

            assertThat(searchRequestCaptor.getValue().indices(), arrayWithSize(1));
            assertThat(searchRequestCaptor.getValue().indices(), arrayContainingInAnyOrder("testsource"));

            Awaitility.await()
                    .atMost(Duration.ofSeconds(2))
                    .until(() -> webhookProvider.getRequestCount() > 0);

            verify(listener, never()).onFailure(any());
            verify(listener).onResponse(executeResponseCaptor.capture());
            ExecuteWatchResponse response = executeResponseCaptor.getValue();
            assertThat(response.getStatus(), equalTo(ExecuteWatchResponse.Status.EXECUTED));
            DocNode result = DocNode.parse(Format.JSON).from(response.getResult().utf8ToString());
            assertThat(result.toJsonString(), result, containsValue("$.status.code", "ACTION_EXECUTED"));
            assertThat(result.toJsonString(), result, containsValue("$.status.detail", "All actions have been executed"));
            assertThat(result.toJsonString(), result, docNodeSizeEqualTo("$.actions", 1));
            assertThat(result.toJsonString(), result, containsValue("$.actions[0].name", "send-http-request"));
            assertThat(result.toJsonString(), result, containsValue("$.actions[0].status.code", "ACTION_EXECUTED"));
        }
    }

    @Test
    public void testExecuteAnonymousWatchWithGoto() throws Exception {
        PlainActionFuture<DocWriteResponse> docWriteResponseFuture = new PlainActionFuture<>();
        docWriteResponseFuture.onResponse(indexResponse("testsink"));
        ArgumentCaptor<IndexRequest> indexRequestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        when(client.index(indexRequestCaptor.capture())).thenReturn(docWriteResponseFuture);

        Watch watch = new WatchBuilder("execution_test_anon").cronTrigger("*/2 * * * * ?").search("testsource").query("{\"match_all\" : {} }")
                .as("testsearch").put("{\"bla\": {\"blub\": 42}, \"x\": \"1\"}").as("teststatic").then().index("testsink").docId("1")
                .refreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).name("testsink").build();

        ExecuteWatchRequest executeWatchRequest = new ExecuteWatchRequest(null, watch.toJson(), false, SimulationMode.FOR_REAL, false);
        executeWatchRequest.setGoTo("teststatic");
        executeWatchAction.doExecute(task, executeWatchRequest, listener);

        verify(client, never()).search(any());

        IndexRequest indexRequest = indexRequestCaptor.getValue();
        DocNode indexRequestSource = DocNode.parse(Format.JSON).from(indexRequest.source().utf8ToString());
        assertThat(indexRequest.index(), equalTo("testsink"));
        assertThat(indexRequestSource.toString(), indexRequestSource, containsOnlyFields("$", "teststatic"));
        assertThat(indexRequestSource.toString(), indexRequestSource, containsValue("$.teststatic.bla.blub", "42"));
        assertThat(indexRequestSource.toString(), indexRequestSource, containsValue("$.teststatic.x", "1"));

        verify(listener, never()).onFailure(any());
        verify(listener).onResponse(executeResponseCaptor.capture());
        ExecuteWatchResponse response = executeResponseCaptor.getValue();
        assertThat(response.getStatus(), equalTo(ExecuteWatchResponse.Status.EXECUTED));
        DocNode result = DocNode.parse(Format.JSON).from(response.getResult().utf8ToString());
        assertThat(result.toJsonString(), result, containsValue("$.status.code", "ACTION_EXECUTED"));
        assertThat(result.toJsonString(), result, containsValue("$.status.detail", "All actions have been executed"));
        assertThat(result.toJsonString(), result, docNodeSizeEqualTo("$.actions", 1));
        assertThat(result.toJsonString(), result, containsValue("$.actions[0].name", "testsink"));
        assertThat(result.toJsonString(), result, containsValue("$.actions[0].status.code", "ACTION_EXECUTED"));
    }

    @Test
    public void testExecuteWatchByIdWhichUsesStoredProxyConfig() throws Exception {
        try (MockWebserviceProvider webhookProvider = new MockWebserviceProvider.Builder("/hook")
                .requiredHttpHeader(REQUEST_HEADER_ADDING_FILTER.getHeader()).build()) {

            when(httpProxyHostRegistry.findHttpProxyHost(STORED_PROXY_ID)).thenReturn(Optional.of(new HttpHost("127.0.0.8", wireMockProxy.port(), "http")));
            when(signalsTenant.getConfigIndexName()).thenReturn(".signals-watch");
            when(signalsTenant.getWatchIdForConfigIndex("execute-by-id-use-stored-proxy")).thenReturn("execute-by-id-use-stored-proxy");

            Watch watch = new WatchBuilder("execute-by-id-use-stored-proxy").cronTrigger("0 0 */1 * * ?")
                    .then().postWebhook(webhookProvider.getUri()).proxy(STORED_PROXY_ID).name("webhook")
                    .build();

            GetRequestBuilder getRequestBuilder = spy(new GetRequestBuilder(client));
            when(client.prepareGet()).thenReturn(getRequestBuilder);
            doAnswer(invocation -> {
                ActionListener<GetResponse> listener = invocation.getArgument(0);
                listener.onResponse(getWatchResponse(watch.toJson()));
                return null;
            }).when(getRequestBuilder).execute(any());

            ExecuteWatchRequest executeWatchRequest = new ExecuteWatchRequest(
                    "execute-by-id-use-stored-proxy", null, false,
                    SimulationMode.FOR_REAL, false
            );
            executeWatchAction.doExecute(task, executeWatchRequest, listener);

            Awaitility.await()
                    .atMost(Duration.ofSeconds(2))
                    .until(() -> webhookProvider.getRequestCount() > 0);

            verify(getRequestBuilder).setId(eq("execute-by-id-use-stored-proxy"));
            verify(getRequestBuilder).setIndex(eq(".signals-watch"));

            verify(listener, never()).onFailure(any());
            verify(listener).onResponse(executeResponseCaptor.capture());
            ExecuteWatchResponse response = executeResponseCaptor.getValue();
            assertThat(response.getStatus(), equalTo(ExecuteWatchResponse.Status.EXECUTED));
            DocNode result = DocNode.parse(Format.JSON).from(response.getResult().utf8ToString());
            assertThat(result.toJsonString(), result, containsValue("$.status.code", "ACTION_EXECUTED"));
            assertThat(result.toJsonString(), result, containsValue("$.status.detail", "All actions have been executed"));
            assertThat(result.toJsonString(), result, docNodeSizeEqualTo("$.actions", 1));
            assertThat(result.toJsonString(), result, containsValue("$.actions[0].name", "webhook"));
            assertThat(result.toJsonString(), result, containsValue("$.actions[0].status.code", "ACTION_EXECUTED"));
        }
    }

    @Test
    public void testExecuteAnonymousWatchWithInput() throws Exception {
        PlainActionFuture<DocWriteResponse> docWriteResponseFuture = new PlainActionFuture<>();
        docWriteResponseFuture.onResponse(indexResponse("testsink"));
        ArgumentCaptor<IndexRequest> indexRequestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        when(client.index(indexRequestCaptor.capture())).thenReturn(docWriteResponseFuture);

        Watch watch = new WatchBuilder("execution_test_anon").cronTrigger("*/2 * * * * ?").search("testsource").query("{\"match_all\" : {} }")
                .as("testsearch").put("{\"bla\": {\"blub\": 42}, \"x\": \"1\"}").as("teststatic").then().index("testsink").docId("1")
                .refreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).name("testsink").build();

        ExecuteWatchRequest executeWatchRequest = new ExecuteWatchRequest(null, watch.toJson(), false, SimulationMode.FOR_REAL, false);
        executeWatchRequest.setGoTo("_actions");
        executeWatchRequest.setInputJson("{ \"ext_input\": \"a\"}");
        executeWatchAction.doExecute(task, executeWatchRequest, listener);

        IndexRequest indexRequest = indexRequestCaptor.getValue();
        DocNode indexRequestSource = DocNode.parse(Format.JSON).from(indexRequest.source().utf8ToString());
        assertThat(indexRequest.index(), equalTo("testsink"));
        assertThat(indexRequest.id(), equalTo("1"));
        assertThat(indexRequestSource.toString(), indexRequestSource, containsOnlyFields("$", "ext_input"));
        assertThat(indexRequestSource.toString(), indexRequestSource, containsValue("$.ext_input", "a"));

        verify(listener, never()).onFailure(any());
        verify(listener).onResponse(executeResponseCaptor.capture());
        ExecuteWatchResponse response = executeResponseCaptor.getValue();
        assertThat(response.getStatus(), equalTo(ExecuteWatchResponse.Status.EXECUTED));
        DocNode result = DocNode.parse(Format.JSON).from(response.getResult().utf8ToString());
        assertThat(result.toJsonString(), result, containsValue("$.status.code", "ACTION_EXECUTED"));
        assertThat(result.toJsonString(), result, containsValue("$.status.detail", "All actions have been executed"));
        assertThat(result.toJsonString(), result, docNodeSizeEqualTo("$.actions", 1));
        assertThat(result.toJsonString(), result, containsValue("$.actions[0].name", "testsink"));
        assertThat(result.toJsonString(), result, containsValue("$.actions[0].status.code", "ACTION_EXECUTED"));
    }

    @Test
    public void testExecuteAnonymousWatchWithShowAllRuntimeAttributes() throws Exception {
        mockTemplateScript(
                new Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, "{\"query\":{\"match_all\":{}}}", Collections.emptyMap()),
                SignalsScriptContextFactory.TEMPLATE_CONTEXT,
                "{\"query\":{\"match_all\":{}}}"
        );
        when(scriptService.compile(
                eq(new Script(ScriptType.INLINE, "painless", "data.testsearch.hits.total.value", Collections.emptyMap())),
                eq(SeverityMapping.SeverityValueScript.CONTEXT)
        )).thenReturn(severityValueScriptFactory);
        when(severityValueScriptFactory.newInstance(any(), any())).thenReturn(severityValueScript);
        when(severityValueScript.execute()).thenReturn(1);
        Watch watch = new WatchBuilder("execute_show_all_runtime_attr").cronTrigger("*/2 * * * * ?").search("testsource").query("{\"match_all\" : {} }")
                .as("testsearch").put("{\"bla\": {\"blub\": 42}}").as("teststatic").consider("data.testsearch.hits.total.value").greaterOrEqual(1)
                .as(SeverityLevel.ERROR).when(SeverityLevel.ERROR).index("testsink").name("testsink").build();

        PlainActionFuture<SearchResponse> searchResponseFuture = new PlainActionFuture<>();
        searchResponseFuture.onResponse(searchResponseWithOneHit());
        ArgumentCaptor<SearchRequest> searchRequestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
        when(client.search(searchRequestCaptor.capture())).thenReturn(searchResponseFuture);

        PlainActionFuture<DocWriteResponse> docWriteResponseFuture = new PlainActionFuture<>();
        docWriteResponseFuture.onResponse(indexResponse("testsink"));
        ArgumentCaptor<IndexRequest> indexRequestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        when(client.index(indexRequestCaptor.capture())).thenReturn(docWriteResponseFuture);

        ExecuteWatchRequest executeWatchRequest = new ExecuteWatchRequest(
                null, watch.toJson(), false, SimulationMode.FOR_REAL, true
        );
        executeWatchAction.doExecute(task, executeWatchRequest, listener);

        assertThat(searchRequestCaptor.getValue().indices(), arrayWithSize(1));
        assertThat(searchRequestCaptor.getValue().indices(), arrayContainingInAnyOrder("testsource"));

        IndexRequest indexRequest = indexRequestCaptor.getValue();
        DocNode indexRequestSource = DocNode.parse(Format.JSON).from(indexRequest.source().utf8ToString());
        assertThat(indexRequest.index(), equalTo("testsink"));
        assertThat(indexRequestSource.toString(), indexRequestSource, containsOnlyFields("$", "testsearch", "teststatic"));
        assertThat(indexRequestSource.toString(), indexRequestSource, containsValue("$.testsearch.hits.hits[0]._source.a", "b"));
        assertThat(indexRequestSource.toString(), indexRequestSource, containsValue("$.teststatic.bla.blub", "42"));

        verify(listener, never()).onFailure(any());
        verify(listener).onResponse(executeResponseCaptor.capture());
        ExecuteWatchResponse response = executeResponseCaptor.getValue();
        assertThat(response.getStatus(), equalTo(ExecuteWatchResponse.Status.EXECUTED));
        DocNode result = DocNode.parse(Format.JSON).from(response.getResult().utf8ToString());
        assertThat(result.toJsonString(), result, containsValue("$.status.code", "ACTION_EXECUTED"));
        assertThat(result.toJsonString(), result, containsValue("$.status.detail", "All actions have been executed"));
        assertThat(result.toJsonString(), result, containsValue("$.status.severity", "error"));
        assertThat(result.toJsonString(), result, docNodeSizeEqualTo("$.actions", 1));
        assertThat(result.toJsonString(), result, containsValue("$.actions[0].name", "testsink"));
        assertThat(result.toJsonString(), result, containsValue("$.actions[0].status.code", "ACTION_EXECUTED"));
        assertThat(result.toJsonString(), result, containsValue("$.runtime_attributes.severity.level", "error"));
        assertThat(result.toJsonString(), result, containsFieldPointedByJsonPath("$.runtime_attributes", "trigger"));
        assertThat(result.toJsonString(), result, containsNullValue("$.runtime_attributes.trigger.triggered_time"));
    }

    @Test
    public void testAckWatchLink() throws Exception {
        mockTemplateScript(
                new Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, "Watch Link: {{ack_watch_link}}\nAction Link: {{ack_action_link}}", Collections.emptyMap()),
                SignalsScriptContextFactory.TEMPLATE_CONTEXT,
                "Watch Link: http://my.frontend/app/searchguard-signals?sg_tenant=SGS_GLOBAL_TENANT#/watch/test_ack_watch_link/ack/\n" +
                        "Action Link: http://my.frontend/app/searchguard-signals?sg_tenant=SGS_GLOBAL_TENANT#/watch/test_ack_watch_link/ack/testaction/"
        );
        mockTemplateScript(
                new Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, "test@test", Collections.emptyMap()),
                SignalsScriptContextFactory.TEMPLATE_CONTEXT,
                "test@test"
        );
        when(scriptService.compile(
                eq(new Script(ScriptType.INLINE, "painless", "data.testdata.a > 0", Collections.emptyMap())),
                eq(Condition.ConditionScript.CONTEXT)
        )).thenReturn(conditionScriptFactory);
        when(conditionScriptFactory.newInstance(any(), any())).thenReturn(conditionScript);
        when(conditionScript.execute()).thenReturn(true);
        when(accountRegistry.lookupAccount(eq("test_ack_watch_link"), eq(EmailAccount.class))).thenReturn(emailAccount("localhost", 9999, "test@test"));
        Watch watch = new WatchBuilder("ack-watch-link").atMsInterval(100000).put("{\"a\": 42}").as("testdata").checkCondition("data.testdata.a > 0")
                .then().email("test").account("test_ack_watch_link").body("Watch Link: {{ack_watch_link}}\nAction Link: {{ack_action_link}}")
                .to("test@test").name("testaction").build();

        GetRequestBuilder getRequestBuilder = spy(new GetRequestBuilder(client));
        when(client.prepareGet()).thenReturn(getRequestBuilder);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(0);
            listener.onResponse(getWatchResponse(watch.toJson()));
            return null;
        }).when(getRequestBuilder).execute(any());

        ExecuteWatchRequest executeWatchRequest = new ExecuteWatchRequest(
                "ack-watch-link", null, false, SimulationMode.SIMULATE_ACTIONS, false
        );
        executeWatchAction.doExecute(task, executeWatchRequest, listener);

        verify(listener, never()).onFailure(any());
        verify(listener).onResponse(executeResponseCaptor.capture());
        ExecuteWatchResponse response = executeResponseCaptor.getValue();
        assertThat(response.getStatus(), equalTo(ExecuteWatchResponse.Status.EXECUTED));
        DocNode result = DocNode.parse(Format.JSON).from(response.getResult().utf8ToString());
        assertThat(result.toJsonString(), result, containsValue("$.status.code", "ACTION_EXECUTED"));
        assertThat(result.toJsonString(), result, containsValue("$.status.detail", "All actions have been executed"));
        assertThat(result.toJsonString(), result, docNodeSizeEqualTo("$.actions", 1));
        assertThat(result.toJsonString(), result, containsValue("$.actions[0].name", "testaction"));
        assertThat(result.toJsonString(), result, containsValue("$.actions[0].status.code", "SIMULATED_ACTION_EXECUTED"));

        String mail = result.findSingleNodeByJsonPath("$.actions[0].request").toString();
        Matcher mailMatcher = Pattern.compile("Watch Link: (\\S+)\nAction Link: (\\S+)", Pattern.MULTILINE).matcher(mail);

        assertThat(mailMatcher.find(), equalTo(true));
        assertThat(result.toJsonString(), "http://my.frontend/app/searchguard-signals?sg_tenant=SGS_GLOBAL_TENANT#/watch/test_ack_watch_link/ack/", equalTo(mailMatcher.group(1)));
        assertThat(result.toJsonString(), "http://my.frontend/app/searchguard-signals?sg_tenant=SGS_GLOBAL_TENANT#/watch/test_ack_watch_link/ack/testaction/", equalTo(mailMatcher.group(2)));
    }

    private void mockTemplateScript(Script script, ScriptContext<TemplateScript.Factory> templateContext, String returnValue) {
        TemplateScript.Factory templateScriptFactory = mock(TemplateScript.Factory.class);
        TemplateScript templateScript = mock(TemplateScript.class);
        when(scriptService.compile(
                eq(script),
                eq(templateContext)
        )).thenReturn(templateScriptFactory);
        when(templateScriptFactory.newInstance(any())).thenReturn(templateScript);
        when(templateScript.execute()).thenReturn(returnValue);
    }

    private EmailAccount emailAccount(String host, int port, String defaultFrom) {
        EmailAccount account = new EmailAccount();
        account.setHost(host);
        account.setPort(port);
        account.setDefaultFrom(defaultFrom);
        return account;
    }

    private GetResponse getWatchResponse(String watchJson) {
        GetResult getResult = new GetResult(
                "", "watch-id", 1, 1, 1, true,
                BytesReference.fromByteBuffer(ByteBuffer.wrap(watchJson.getBytes(StandardCharsets.UTF_8))),
                null, null
        );
        return new GetResponse(getResult);
    }

    private DocWriteResponse indexResponse(String index) {
        return new IndexResponse(new ShardId(index, "uuid", 1), "1", 1, 1, 1, true);
    }

    private SearchResponse searchResponseWithOneHit() {
        BytesReference source = BytesReference.fromByteBuffer(ByteBuffer.wrap(DocNode.of("a", "b").toBytes(Format.JSON)));
        SearchHit[] hits = new SearchHit[] {SearchHit.unpooled(1).sourceRef(source)};
        TotalHits totalHits = new TotalHits(1, TotalHits.Relation.EQUAL_TO);
        SearchHits searchHits = SearchHits.unpooled(hits, totalHits, 1);
        return new SearchResponse(
                searchHits, null, null, false, false, null,
                0, null, 1, 1, 0, 10,
                ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY
        );
    }

    private X509ExtendedTrustManager trustManagerFromPem(String pemContent) throws Exception {
        Collection<? extends Certificate> certificates = parseCertificates(pemContent);
        KeyStore truststore = toTruststore(STORED_TRUSTSTORE_ID, certificates);
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(truststore);
        TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
        List<TrustManager> x509TrustManagers = Arrays.stream(trustManagers)//
                .filter(X509ExtendedTrustManager.class::isInstance)//
                .collect(Collectors.toList());//
        if(x509TrustManagers.size() != 1) {
            throw new RuntimeException("Incorrect number of x509 trust managers: " + x509TrustManagers.size());
        }
        return (X509ExtendedTrustManager) x509TrustManagers.get(0);
        }

}
