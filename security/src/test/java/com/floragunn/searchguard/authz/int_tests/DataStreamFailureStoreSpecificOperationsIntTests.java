/*
 * Copyright 2026 floragunn GmbH
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

package com.floragunn.searchguard.authz.int_tests;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestComponentTemplate;
import com.floragunn.searchguard.test.TestDataStream;
import com.floragunn.searchguard.test.TestIndexTemplate;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.floragunn.searchguard.test.RestMatchers.isCreated;
import static com.floragunn.searchguard.test.RestMatchers.isForbidden;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsFieldPointedByJsonPath;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.valueSatisfiesMatcher;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

@RunWith(Suite.class)
@Suite.SuiteClasses({ DataStreamFailureStoreSpecificOperationsIntTests.RedirectToFailureStore.class, DataStreamFailureStoreSpecificOperationsIntTests.FailureStoreOperations.class })
public class DataStreamFailureStoreSpecificOperationsIntTests {

    static TestDataStream ds_aw1 = TestDataStream.name("ds_aw1").documentCount(22).rolloverAfter(10).build();

    static TestSgConfig.User LIMITED_USER_A = new TestSgConfig.User("limited_user_A")//
            .description("read ds_a*, write ds_aw* without failures")//
            .roles(//
                    new TestSgConfig.Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS")//
                            .dataStreamPermissions("SGS_READ").on("ds_a*")//
                            .dataStreamPermissions("SGS_WRITE", "SGS_MANAGE").on("ds_aw*"));

    static TestSgConfig.User LIMITED_USER_A_WITH_FAILURE_STORE = new TestSgConfig.User("limited_user_A_with_failure_store")//
            .description("read/write ds_a* including failures")//
            .roles(//
                    new TestSgConfig.Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS")//
                            .dataStreamPermissions("SGS_WRITE", "SGS_READ", "SGS_MANAGE", "special:failure_store").on("ds_a*")
            );

    static TestSgConfig.User ADMIN_CERT_USER = new TestSgConfig.User("admin cert")
            .description("admin cert user")
            .adminCertUser();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled()
            .users(LIMITED_USER_A, LIMITED_USER_A_WITH_FAILURE_STORE, ADMIN_CERT_USER)//
            .indexTemplates(new TestIndexTemplate("ds_test", "ds_*").dataStream().composedOf(TestComponentTemplate.DATA_STREAM_MINIMAL))//
            .dataStreams(ds_aw1)//
            .authzDebug(true)//
            .useExternalProcessCluster().build();

    @BeforeClass
    public static void createIngestPipeline() throws Exception {
        createOrUpdateIngestPipeline("copy-field-pipeline", DocNode.of("set.field", "a-copy", "set.copy_from", "a"));
    }

    public static class RedirectToFailureStore {

        @Test
        public void indexValidDoc_shouldNotRedirectDocToFailureStore() throws Exception {
            try (GenericRestClient adminCertClient = cluster.getRestClient(ADMIN_CERT_USER);
                 GenericRestClient limitedDsAClient = cluster.getRestClient(LIMITED_USER_A)) {

                DocNode docNode = buildDoc();
                GenericRestClient.HttpResponse response = indexSingleDoc(adminCertClient, ds_aw1, "", docNode);
                assertThatIndexedDocWasNotRedirectedToFailureStore(response);

                response = indexSingleDoc(limitedDsAClient, ds_aw1, "", docNode);
                assertThatIndexedDocWasNotRedirectedToFailureStore(response);
            }
        }

        @Test
        public void indexValidDoc_withIngestPipeline_pipelineSucceeds_shouldNotRedirectDocToFailureStore() throws Exception {
            try (GenericRestClient adminCertClient = cluster.getRestClient(ADMIN_CERT_USER);
                 GenericRestClient limitedDsAClient = cluster.getRestClient(LIMITED_USER_A)) {

                DocNode docNode = buildDoc();
                GenericRestClient.HttpResponse response = indexSingleDoc(adminCertClient, ds_aw1, "pipeline=copy-field-pipeline", docNode);
                assertThatIndexedDocWasNotRedirectedToFailureStore(response);

                response = indexSingleDoc(limitedDsAClient, ds_aw1, "pipeline=copy-field-pipeline", docNode);
                assertThatIndexedDocWasNotRedirectedToFailureStore(response);
            }
        }

        @Test
        public void indexValidDoc_withIngestPipeline_pipelineFails_shouldRedirectDocToFailureStore() throws Exception {
            try (GenericRestClient adminCertClient = cluster.getRestClient(ADMIN_CERT_USER);
                 GenericRestClient limitedDsAClient = cluster.getRestClient(LIMITED_USER_A)) {

                // remove the field which the ingest pipeline will try to copy
                DocNode docNode = buildDoc().without("a");

                GenericRestClient.HttpResponse response = indexSingleDoc(adminCertClient, ds_aw1, "pipeline=copy-field-pipeline", docNode);
                assertThatIndexedDocWasRedirectedToFailureStore(response);

                response = indexSingleDoc(limitedDsAClient, ds_aw1, "pipeline=copy-field-pipeline", docNode);
                assertThatIndexedDocWasRedirectedToFailureStore(response);
            }
        }

        @Test
        public void indexInvalidDoc_shouldRedirectDocToFailureStore() throws Exception {
            try (GenericRestClient adminCertClient = cluster.getRestClient(ADMIN_CERT_USER);
                 GenericRestClient limitedDsAClient = cluster.getRestClient(LIMITED_USER_A)) {

                // set an invalid value for the @timestamp field
                DocNode docNode = buildDoc().with("@timestamp", "asd");

                GenericRestClient.HttpResponse response = indexSingleDoc(adminCertClient, ds_aw1, "", docNode);
                assertThatIndexedDocWasRedirectedToFailureStore(response);

                response = indexSingleDoc(limitedDsAClient, ds_aw1, "", docNode);
                assertThatIndexedDocWasRedirectedToFailureStore(response);
            }
        }

        @Test
        public void indexInvalidDoc_withIngestPipeline_shouldRedirectDocToFailureStore() throws Exception {
            try (GenericRestClient adminCertClient = cluster.getRestClient(ADMIN_CERT_USER);
                 GenericRestClient limitedDsAClient = cluster.getRestClient(LIMITED_USER_A)) {

                // set an invalid value for the @timestamp field
                DocNode docNode = buildDoc().with("@timestamp", "asd");

                GenericRestClient.HttpResponse response = indexSingleDoc(adminCertClient, ds_aw1, "pipeline=copy-field-pipeline", docNode);
                assertThatIndexedDocWasRedirectedToFailureStore(response);

                response = indexSingleDoc(limitedDsAClient, ds_aw1, "pipeline=copy-field-pipeline", docNode);
                assertThatIndexedDocWasRedirectedToFailureStore(response);
            }
        }

        @Test
        public void bulkIndexValidDocs_shouldNotRedirectDocsToFailureStore() throws Exception {
            try (GenericRestClient adminCertClient = cluster.getRestClient(ADMIN_CERT_USER);
                 GenericRestClient limitedDsAClient = cluster.getRestClient(LIMITED_USER_A)) {

                DocNode[] docs = new DocNode[] {buildDoc(), buildDoc()};
                GenericRestClient.HttpResponse response = indexDocs(adminCertClient, ds_aw1, "", docs);
                assertThatBulkIndexedDocsWereNotRedirectedToFailureStore(response);

                response = indexDocs(limitedDsAClient, ds_aw1, "", docs);
                assertThatBulkIndexedDocsWereNotRedirectedToFailureStore(response);
            }
        }

        @Test
        public void bulkIndexInvalidDocs_shouldRedirectDocsToFailureStore() throws Exception {
            try (GenericRestClient adminCertClient = cluster.getRestClient(ADMIN_CERT_USER);
                 GenericRestClient limitedDsAClient = cluster.getRestClient(LIMITED_USER_A)) {

                // set an invalid value for the @timestamp field
                DocNode[] docs = new DocNode[] {buildDoc().with("@timestamp", "asd"), buildDoc().with("@timestamp", "asd")};

                GenericRestClient.HttpResponse response = indexDocs(adminCertClient, ds_aw1, "", docs);
                assertThatBulkIndexedDocsWereRedirectedToFailureStore(response);

                response = indexDocs(limitedDsAClient, ds_aw1, "", docs);
                assertThatBulkIndexedDocsWereRedirectedToFailureStore(response);
            }
        }

        @Test
        public void bulkIndexValidDocs_withIngestPipeline_pipelineFails_shouldRedirectDocToFailureStore() throws Exception {
            try (GenericRestClient adminCertClient = cluster.getRestClient(ADMIN_CERT_USER);
                 GenericRestClient limitedDsAClient = cluster.getRestClient(LIMITED_USER_A)) {

                // remove the field which the ingest pipeline will try to copy
                DocNode[] docs = new DocNode[] {buildDoc().without("a"), buildDoc().without("a")};
                GenericRestClient.HttpResponse response = indexDocs(adminCertClient, ds_aw1, "pipeline=copy-field-pipeline", docs);
                assertThatBulkIndexedDocsWereRedirectedToFailureStore(response);

                response = indexDocs(limitedDsAClient, ds_aw1, "pipeline=copy-field-pipeline", docs);
                assertThatBulkIndexedDocsWereRedirectedToFailureStore(response);
            }
        }

    }

    @RunWith(Parameterized.class)
    public static class FailureStoreOperations {

        final TestSgConfig.User user;

        static List<TestSgConfig.User> USERS = ImmutableList.of(
                LIMITED_USER_A, LIMITED_USER_A_WITH_FAILURE_STORE, ADMIN_CERT_USER
        );

        public FailureStoreOperations(TestSgConfig.User user, String description) {
            this.user = user;
        }

        @Parameterized.Parameters(name = "{1}")
        public static Collection<Object[]> params() {
            List<Object[]> result = new ArrayList<>();

            for (TestSgConfig.User user : DataStreamFailureStoreSpecificOperationsIntTests.FailureStoreOperations.USERS) {
                result.add(new Object[]{user, user.getDescription()});
            }

            return result;
        }

        @Test
        public void reindexDocsRedirectedToFailureStore() throws Exception {
            try (GenericRestClient client = cluster.getRestClient(user)) {
                String dsName = "ds_aw_reindex_docs_redirected_to_failure_store";
                String copyFieldPipeline = "reindex-test-copy-field-pipeline";
                String restoreOriginDocPipeline = "reindex-test-restore-origin-doc-pipeline";

                try {
                    //create a ds and pipelines for this test
                    createDataStream(dsName);
                    createOrUpdateIngestPipeline(copyFieldPipeline,
                            DocNode.of("set.field", "c-copy", "set.copy_from", "c"),
                            DocNode.of("set.field", "setField", "set.value", 1)
                    );
                    createOrUpdateRestoreOriginDocIngestPipeline(restoreOriginDocPipeline, copyFieldPipeline, dsName);

                    // docs do not contain the 'c' field which the pipeline will try to copy
                    DocNode[] docs = new DocNode[] {buildDoc().with("b", 1), buildDoc().with("b", 2)};
                    GenericRestClient.HttpResponse response = indexDocs(client, dsName, "pipeline=" + copyFieldPipeline, docs);
                    assertThatBulkIndexedDocsWereRedirectedToFailureStore(response);

                    //fix the pipeline, copy the 'c' field only if it is present
                    createOrUpdateIngestPipeline(copyFieldPipeline,
                            DocNode.of("set.field", "a-copy", "set.copy_from", "c", "set.if", "ctx.c != null"),
                            DocNode.of("set.field", "setField", "set.value", 1)
                    );

                    //run reindex operation
                    response = client.postJson("_reindex?refresh=true", """
                         {
                            "source": {
                              "index": "%s"
                            },
                            "dest": {
                              "index": "%s",
                              "op_type": "create",
                              "pipeline": "%s"
                            }
                          }
                        """.formatted(dsName + "::failures", dsName, restoreOriginDocPipeline));
                    if (user == LIMITED_USER_A) {
                        assertThat("_reindex, user %s should get 403 response".formatted(user.getName()), response, isForbidden());
                        return;
                    }
                    assertThat("_reindex, user %s should get 200 response".formatted(user.getName()), response, isOk());
                    assertThat(response.getBodyAsDocNode(), containsValue("$.created", 2));

                    List<DocNode> expectedProcessedDocs = Stream.of(docs).map(doc -> doc.with("setField", 1)).toList();
                    response = client.get("/" + dsName + "/_search");
                    assertThat(response.getBodyAsDocNode(), containsValue("$.hits.total.value", 2L));
                    List<DocNode> docsFromHits = response.getBodyAsDocNode().findNodesByJsonPath("$.hits.hits[*]._source");
                    assertThat(docsFromHits, containsInAnyOrder(expectedProcessedDocs.toArray()));
                } finally {
                    deleteDataStream(dsName);
                    deleteIngestPipeline(copyFieldPipeline);
                    deleteIngestPipeline(restoreOriginDocPipeline);
                }
            }
        }

        @Test
        public void rolloverDataStreamAndFailureStore() throws Exception {
            try (GenericRestClient client = cluster.getRestClient(user)) {
                GenericRestClient.HttpResponse response = client.post(ds_aw1.getName() + "/_rollover");
                assertThat(response, isOk());
                response = client.post(ds_aw1.getName() + "::failures/_rollover");
                if (user == LIMITED_USER_A) {
                    assertThat("_rollover ::failures, user %s should get 403 response".formatted(user.getName()), response, isForbidden());
                } else {
                    assertThat("_rollover ::failures, user %s should get 200 response".formatted(user.getName()), response, isOk());
                }
            }
        }
    }

    private static void createOrUpdateIngestPipeline(String name, DocNode... processors) throws Exception{
        try (GenericRestClient adminCertClient = cluster.getAdminCertRestClient()) {
            String processorsArrayString = Stream.of(processors).map(DocNode::toJsonString).collect(Collectors.joining(",", "[", "]"));
            GenericRestClient.HttpResponse response = adminCertClient.putJson("/_ingest/pipeline/" + name, """
                    {
                      "processors": %s
                    }
                    """.formatted(processorsArrayString));
            assertThat(response, isOk());
        }
    }

    private static void createOrUpdateRestoreOriginDocIngestPipeline(String name, String nextPipelineName, String rerouteTo) throws Exception {
        DocNode restoreProcessor = DocNode.of("script.lang", "painless", "script.source", """
                ctx._index = ctx.document.index;
                def source = ctx.document.source;
                ctx.remove("error");
                ctx.remove("document");
                for (entry in source.entrySet()) {
                    ctx[entry.key] = entry.value;
                }
                """);
        DocNode callNextPipelineProcessor = DocNode.of("pipeline.name", nextPipelineName);
        DocNode rerouteProcessor = DocNode.of("reroute.destination", rerouteTo);
        createOrUpdateIngestPipeline(name, restoreProcessor, callNextPipelineProcessor, rerouteProcessor);
    }

    private static void deleteIngestPipeline(String name) throws Exception{
        try (GenericRestClient adminCertClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse response = adminCertClient.delete("/_ingest/pipeline/" + name);
            assertThat(response, isOk());
        }
    }

    private static void createDataStream(String name) throws Exception {
        try (GenericRestClient adminCertClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse response = adminCertClient.put("/_data_stream/" + name);
            assertThat(response, isOk());
        }
    }

    private static void deleteDataStream(String name) throws Exception {
        try (GenericRestClient adminCertClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse response = adminCertClient.delete("/_data_stream/" + name);
            assertThat(response, isOk());
        }
    }

    private static GenericRestClient.HttpResponse indexSingleDoc(GenericRestClient client, TestDataStream dataStream,
                                                          String requestParams, DocNode doc) throws Exception {
        String params = StringUtils.isEmpty(requestParams) ? "" : "&" + requestParams;
        return client.postJson("/"+ dataStream.getName() + "/_doc/?refresh=true" + params, doc);
    }

    private static GenericRestClient.HttpResponse indexDocs(GenericRestClient client, TestDataStream dataStream,
                                                     String requestParams, DocNode... docs) throws Exception {
        return indexDocs(client, dataStream.getName(), requestParams, docs);
    }

    private static GenericRestClient.HttpResponse indexDocs(GenericRestClient client, String dataStream,
                                                     String requestParams, DocNode... docs) throws Exception {
        StringBuilder sb = new StringBuilder();
        for (DocNode doc : docs) {
            sb.append("{\"create\":{}}\n");
            sb.append(doc.toJsonString());
            sb.append("\n");
        }
        String docsStr = sb.toString();

        String params = StringUtils.isEmpty(requestParams) ? "" : "&" + requestParams;
        return client.postJson("/" + dataStream + "/_bulk?refresh=true" + params, docsStr);
    }

    private static DocNode buildDoc() {
        return DocNode.of("a", 1, "@timestamp", Instant.now().toString());
    }

    private static void assertThatIndexedDocWasNotRedirectedToFailureStore(GenericRestClient.HttpResponse response) throws Exception {
        assertThat(response, isCreated());
        assertThat(response.getBody(), response.getBodyAsDocNode(), not(containsFieldPointedByJsonPath("$", "failure_store")));
        assertThat(response.getBody(), response.getBodyAsDocNode(), valueSatisfiesMatcher("$._index", String.class, startsWith(".ds")));
    }

    private static void assertThatIndexedDocWasRedirectedToFailureStore(GenericRestClient.HttpResponse response) throws Exception {
        assertThat(response, isCreated());
        assertThat(response.getBody(), response.getBodyAsDocNode(), containsFieldPointedByJsonPath("$", "failure_store"));
        assertThat(response.getBody(), response.getBodyAsDocNode(), valueSatisfiesMatcher("$._index", String.class, startsWith(".fs")));
    }

    private static void assertThatBulkIndexedDocsWereNotRedirectedToFailureStore(GenericRestClient.HttpResponse response) throws Exception{
        assertThat(response, isOk());
        List<DocNode> indexDocsResponses = response.getBodyAsDocNode().findNodesByJsonPath("$.items[*]");
        assertThat(indexDocsResponses, not(empty()));
        for (DocNode indexDocResponse : indexDocsResponses) {
            assertThat(indexDocResponse.toJsonString(), indexDocResponse, valueSatisfiesMatcher("$.create._index", String.class, startsWith(".ds")));
            assertThat(indexDocResponse.toJsonString(), indexDocResponse, not(containsFieldPointedByJsonPath("$.create", "failure_store")));
        }
    }

    private static void assertThatBulkIndexedDocsWereRedirectedToFailureStore(GenericRestClient.HttpResponse response) throws Exception{
        assertThat(response, isOk());
        List<DocNode> indexDocsResponses = response.getBodyAsDocNode().findNodesByJsonPath("$.items[*]");
        assertThat(indexDocsResponses, not(empty()));
        for (DocNode indexDocResponse : indexDocsResponses) {
            assertThat(indexDocResponse.toJsonString(), indexDocResponse, valueSatisfiesMatcher("$.create._index", String.class, startsWith(".fs")));
            assertThat(indexDocResponse.toJsonString(), indexDocResponse, containsFieldPointedByJsonPath("$.create", "failure_store"));
        }
    }
}
