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

import java.time.Instant;
import java.util.List;

import static com.floragunn.searchguard.test.RestMatchers.isCreated;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsFieldPointedByJsonPath;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.valueSatisfiesMatcher;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class DataStreamFailureStoreRedirectIntTests {

    static TestDataStream ds_aw1 = TestDataStream.name("ds_aw1").documentCount(22).rolloverAfter(10).build();

    static TestSgConfig.User LIMITED_USER_A = new TestSgConfig.User("limited_user_A")//
            .description("ds_a*")//
            .roles(//
                    new TestSgConfig.Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .dataStreamPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*").on("ds_a*")//
                            .dataStreamPermissions("SGS_WRITE").on("ds_aw*"));

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().users(LIMITED_USER_A)//
            .indexTemplates(new TestIndexTemplate("ds_test", "ds_*").dataStream().composedOf(TestComponentTemplate.DATA_STREAM_MINIMAL))//
            .dataStreams(ds_aw1)//
            .authzDebug(true)//
            .useExternalProcessCluster().build();

    @BeforeClass
    public static void createIngestPipeline() throws Exception {
        try (GenericRestClient adminCertClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse response = adminCertClient.putJson("/_ingest/pipeline/copy-field-pipeline", """
                    {
                      "description": "pipeline - copy from field 'a' to field 'a-copy'",
                      "processors": [
                        {
                          "set": {
                            "field": "a-copy",
                            "copy_from": "a"
                          }
                        }
                      ]
                    }
                    """);
            assertThat(response, isOk());
        }
    }

    @Test
    public void indexValidDoc() throws Exception {
        try (GenericRestClient adminCertClient = cluster.getAdminCertRestClient();
             GenericRestClient limitedDsAClient = cluster.getRestClient(LIMITED_USER_A)) {

            DocNode docNode = buildDoc();
            GenericRestClient.HttpResponse response = indexSingleDoc(adminCertClient, ds_aw1, "", docNode);
            assertThatIndexedDocWasNotRedirectedToFailureStore(response);

            response = indexSingleDoc(limitedDsAClient, ds_aw1, "", docNode);
            assertThatIndexedDocWasNotRedirectedToFailureStore(response);
        }
    }

    @Test
    public void indexValidDoc_withIngestPipeline_pipelineSucceeds() throws Exception {
        try (GenericRestClient adminCertClient = cluster.getAdminCertRestClient();
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
        try (GenericRestClient adminCertClient = cluster.getAdminCertRestClient();
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
        try (GenericRestClient adminCertClient = cluster.getAdminCertRestClient();
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
        try (GenericRestClient adminCertClient = cluster.getAdminCertRestClient();
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
    public void bulkIndexValidDocs() throws Exception {
        try (GenericRestClient adminCertClient = cluster.getAdminCertRestClient();
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
        try (GenericRestClient adminCertClient = cluster.getAdminCertRestClient();
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
        try (GenericRestClient adminCertClient = cluster.getAdminCertRestClient();
             GenericRestClient limitedDsAClient = cluster.getRestClient(LIMITED_USER_A)) {

            // remove the field which the ingest pipeline will try to copy
            DocNode[] docs = new DocNode[] {buildDoc().without("a"), buildDoc().without("a")};
            GenericRestClient.HttpResponse response = indexDocs(adminCertClient, ds_aw1, "pipeline=copy-field-pipeline", docs);
            assertThatBulkIndexedDocsWereRedirectedToFailureStore(response);

            response = indexDocs(limitedDsAClient, ds_aw1, "pipeline=copy-field-pipeline", docs);
            assertThatBulkIndexedDocsWereRedirectedToFailureStore(response);
        }
    }

    private GenericRestClient.HttpResponse indexSingleDoc(GenericRestClient client, TestDataStream dataStream,
                                                          String requestParams, DocNode doc) throws Exception {
        String params = StringUtils.isEmpty(requestParams) ? "" : "?" + requestParams;
        return client.postJson("/"+ dataStream.getName() + "/_doc/" + params, doc);
    }

    private GenericRestClient.HttpResponse indexDocs(GenericRestClient client, TestDataStream dataStream,
                                                     String requestParams, DocNode... docs) throws Exception {
        StringBuilder sb = new StringBuilder();
        for (DocNode doc : docs) {
            sb.append("{\"create\":{}}\n");
            sb.append(doc.toJsonString());
            sb.append("\n");
        }
        String docsStr = sb.toString();

        String params = StringUtils.isEmpty(requestParams) ? "" : "?" + requestParams;
        return client.postJson("/" + dataStream.getName() + "/_bulk" + params, docsStr);
    }

    private DocNode buildDoc() {
        return DocNode.of("a", 1, "@timestamp", Instant.now().toString());
    }

    private void assertThatIndexedDocWasNotRedirectedToFailureStore(GenericRestClient.HttpResponse response) throws Exception {
        assertThat(response, isCreated());
        assertThat(response.getBody(), response.getBodyAsDocNode(), not(containsFieldPointedByJsonPath("$", "failure_store")));
        assertThat(response.getBody(), response.getBodyAsDocNode(), valueSatisfiesMatcher("$._index", String.class, startsWith(".ds")));
    }

    private void assertThatIndexedDocWasRedirectedToFailureStore(GenericRestClient.HttpResponse response) throws Exception {
        assertThat(response, isCreated());
        assertThat(response.getBody(), response.getBodyAsDocNode(), containsFieldPointedByJsonPath("$", "failure_store"));
        assertThat(response.getBody(), response.getBodyAsDocNode(), valueSatisfiesMatcher("$._index", String.class, startsWith(".fs")));
    }

    private void assertThatBulkIndexedDocsWereNotRedirectedToFailureStore(GenericRestClient.HttpResponse response) throws Exception{
        assertThat(response, isOk());
        List<DocNode> indexDocsResponses = response.getBodyAsDocNode().findNodesByJsonPath("$.items[*]");
        assertThat(indexDocsResponses, not(empty()));
        for (DocNode indexDocResponse : indexDocsResponses) {
            assertThat(indexDocResponse.toJsonString(), indexDocResponse, valueSatisfiesMatcher("$.create._index", String.class, startsWith(".ds")));
            assertThat(indexDocResponse.toJsonString(), indexDocResponse, not(containsFieldPointedByJsonPath("$.create", "failure_store")));
        }
    }

    private void assertThatBulkIndexedDocsWereRedirectedToFailureStore(GenericRestClient.HttpResponse response) throws Exception{
        assertThat(response, isOk());
        List<DocNode> indexDocsResponses = response.getBodyAsDocNode().findNodesByJsonPath("$.items[*]");
        assertThat(indexDocsResponses, not(empty()));
        for (DocNode indexDocResponse : indexDocsResponses) {
            assertThat(indexDocResponse.toJsonString(), indexDocResponse, valueSatisfiesMatcher("$.create._index", String.class, startsWith(".fs")));
            assertThat(indexDocResponse.toJsonString(), indexDocResponse, containsFieldPointedByJsonPath("$.create", "failure_store"));
        }
    }
}
