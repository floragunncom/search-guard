package com.floragunn.searchguard.enterprise.dlsfls;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestData;
import com.floragunn.searchguard.test.TestSgConfig.Authc;
import com.floragunn.searchguard.test.TestSgConfig.DlsFls;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.TestSgConfig.User;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentType;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsFieldPointedByJsonPath;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.docNodeSizeEqualTo;
import static java.util.Arrays.asList;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@RunWith(Parameterized.class)
public class FlsKeywordTest {
    private static final Logger log = LogManager.getLogger(FlsKeywordTest.class);
    static final String INDEX_NAME_PREFIX = "documents";
    static final String INDEX_PATTERN = INDEX_NAME_PREFIX + "*";
    static final String INDEX_NORMAL_MODE = INDEX_NAME_PREFIX + "_normal_index_mode";
    static final String INDEX_LOGS_DB_MODE = INDEX_NAME_PREFIX + "_logs_db_index_mode";
    public static final String FIELD_AUTHOR = "author";
    public static final String FIELD_AUTHOR_KEYWORD = "author.keyword";
    public static final String FIELD_TITLE = "title";
    public static final String FIELD_TITLE_KEYWORD = "title.keyword";
    public static final String FIELD_CONTENT = "content";
    public static final String FIELD_CONTENT_KEYWORD = "content.keyword";
    public static final String AUTHOR_AGGREGATION_NAME = "author-aggregation";
    public static final String MATCH_ALL_QUERY_WITH_FIELDS = DocNode.of("query", DocNode.of("match_all", DocNode.EMPTY), "fields", asList(FIELD_TITLE, FIELD_TITLE_KEYWORD, FIELD_CONTENT, FIELD_CONTENT_KEYWORD, FIELD_AUTHOR, FIELD_AUTHOR_KEYWORD), "_source", false).toJsonString();
    public static final String AGGREGATION_BY_AUTHOR_QUERY = DocNode.of("size", 0, "aggs", DocNode.of(AUTHOR_AGGREGATION_NAME, DocNode.of("terms", DocNode.of("field", FIELD_AUTHOR_KEYWORD)))).toJsonString();
    public static final String AUTHOR_GOETHE = "Goethe";

    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    static final Authc AUTHC = new Authc(new Authc.Domain("basic/internal_users_db"));
    static final DlsFls DLSFLS = new DlsFls().useImpl("flx").metrics("detailed");

    private static Role DOCUMENT_READER_ROLE = new Role("document_reader_role").clusterPermissions("*")
            .indexPermissions("indices:data/read/search").on(INDEX_PATTERN);

    private static Role DOCUMENT_READER_EXCEPT_AUTHOR_ROLE = new Role("document_reader_except_author_role").clusterPermissions("*")
        .indexPermissions("indices:data/read/search").fls("~" + FIELD_AUTHOR).on(INDEX_PATTERN);

    private static Role DOCUMENT_TITLE_READER_ROLE = new Role("document_title_reader_role").clusterPermissions("*")
        .indexPermissions("indices:data/read/search").fls(FIELD_TITLE).on(INDEX_PATTERN);

    private static User DOCUMENT_READER_USER = new User("document_reader_user").roles(DOCUMENT_READER_ROLE.getName());
    private static User DOCUMENT_READER_EXCEPT_AUTHOR_USER = new User("document_reader_except_author_user").roles(DOCUMENT_READER_EXCEPT_AUTHOR_ROLE.getName());
    private static User DOCUMENT_TITLE_READER_USER = new User("document_title_reader_user").roles(DOCUMENT_TITLE_READER_ROLE.getName());

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().enterpriseModulesEnabled().authc(AUTHC).dlsFls(DLSFLS)
        .roles(DOCUMENT_READER_ROLE, DOCUMENT_READER_EXCEPT_AUTHOR_ROLE, DOCUMENT_TITLE_READER_ROLE)
        .users(DOCUMENT_READER_USER, DOCUMENT_READER_EXCEPT_AUTHOR_USER, DOCUMENT_TITLE_READER_USER).build();

    private final String documentsIndexName;

    public FlsKeywordTest(String documentsIndexName) {
        this.documentsIndexName = documentsIndexName;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Object[] parameters() {
        return new Object[] { INDEX_NORMAL_MODE, INDEX_LOGS_DB_MODE };
    }

    @BeforeClass
    public static void populateData() {
        Client client = cluster.getPrivilegedInternalNodeClient();
        PutIndexTemplateRequest templateRequest = new PutIndexTemplateRequest("logsdb_template") //
                .patterns(ImmutableList.of(INDEX_LOGS_DB_MODE)) //
                .settings(Settings.builder().put("index.mode", "logsdb").build());
        AcknowledgedResponse acknowledgedResponse = client.admin().indices().putTemplate(templateRequest).actionGet();
        assertThat(acknowledgedResponse.isAcknowledged(), equalTo(true));
        String documentContent = DocNode.of(FIELD_TITLE, "accessible document title", //
                FIELD_CONTENT, "document content",  //
                "@timestamp", "2022-01-01T00:00:00Z", //
                FIELD_AUTHOR, AUTHOR_GOETHE) //
                .toJsonString();
        for(String indexName : List.of(INDEX_NORMAL_MODE, INDEX_LOGS_DB_MODE)) {
            IndexRequest indexRequest = new IndexRequest(indexName) //
                    .id("0") //
                    .setRefreshPolicy(IMMEDIATE) //
                    .source(documentContent, XContentType.JSON);
            DocWriteResponse docWriteResponse = client.index(indexRequest).actionGet();
            assertThat(docWriteResponse.getResult(), equalTo(DocWriteResponse.Result.CREATED));
        }
        // null means default mode which is currently normal
        assertThat(TestData.getIndexMode(client, INDEX_NORMAL_MODE), anyOf(equalTo("normal"), nullValue()));
        assertThat(TestData.getIndexMode(client, INDEX_LOGS_DB_MODE), equalTo("logsdb"));
    }

    @Test
    public void shouldAccessAuthorFieldDuringRegularSearch() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(DOCUMENT_READER_USER)) {
            HttpResponse response = client.get("/" + documentsIndexName + "/_search");

            assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(response.getBodyAsDocNode(), containsFieldPointedByJsonPath("hits.hits[0]._source", FIELD_AUTHOR));
            assertThat(response.getBodyAsDocNode(), containsValue("hits.hits[0]._source.author", AUTHOR_GOETHE));
        }
    }

    @Test
    public void shouldNotAccessAuthorFieldWhenUserIsLackingFlPermission() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(DOCUMENT_READER_EXCEPT_AUTHOR_USER)) {
            HttpResponse response = client.get("/" + documentsIndexName + "/_search");

            assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(response.getBodyAsDocNode(), not(containsFieldPointedByJsonPath("hits.hits[0]._source", FIELD_AUTHOR)));
        }
    }

    @Test
    public void shouldReadWholeDocument() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(DOCUMENT_READER_USER)) {
            HttpResponse response = client.postJson("/" + documentsIndexName + "/_search", MATCH_ALL_QUERY_WITH_FIELDS);

            assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(response.getBodyAsDocNode(), containsFieldPointedByJsonPath("hits.hits[0].fields", FIELD_TITLE));
            assertThat(response.getBodyAsDocNode(), containsFieldPointedByJsonPath("hits.hits[0].fields", FIELD_TITLE_KEYWORD));
            assertThat(response.getBodyAsDocNode(), containsFieldPointedByJsonPath("hits.hits[0].fields", FIELD_CONTENT));
            assertThat(response.getBodyAsDocNode(), containsFieldPointedByJsonPath("hits.hits[0].fields", FIELD_CONTENT_KEYWORD));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("hits.hits[0].fields", 6));
        }
    }

    @Test
    public void shouldReadOnlyTitleFromDocument() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(DOCUMENT_TITLE_READER_USER)) {
            HttpResponse response = client.postJson("/" + documentsIndexName + "/_search", MATCH_ALL_QUERY_WITH_FIELDS);

            assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(response.getBodyAsDocNode(), containsFieldPointedByJsonPath("hits.hits[0].fields", FIELD_TITLE));
            assertThat(response.getBodyAsDocNode(), containsFieldPointedByJsonPath("hits.hits[0].fields", FIELD_TITLE_KEYWORD));
            assertThat(response.getBodyAsDocNode(), not(containsFieldPointedByJsonPath("hits.hits[0].fields", FIELD_CONTENT)));
            assertThat(response.getBodyAsDocNode(), not(containsFieldPointedByJsonPath("hits.hits[0].fields", FIELD_CONTENT_KEYWORD)));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("hits.hits[0].fields", 2));
        }
    }

    @Test
    public void shouldReadEverythingBesidesAuthor() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(DOCUMENT_READER_EXCEPT_AUTHOR_USER)) {
            HttpResponse response = client.postJson("/" + documentsIndexName + "/_search", MATCH_ALL_QUERY_WITH_FIELDS);

            assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(response.getBodyAsDocNode(), containsFieldPointedByJsonPath("hits.hits[0].fields", FIELD_TITLE));
            assertThat(response.getBodyAsDocNode(), containsFieldPointedByJsonPath("hits.hits[0].fields", FIELD_TITLE_KEYWORD));
            assertThat(response.getBodyAsDocNode(), containsFieldPointedByJsonPath("hits.hits[0].fields", FIELD_CONTENT));
            assertThat(response.getBodyAsDocNode(), containsFieldPointedByJsonPath("hits.hits[0].fields", FIELD_CONTENT_KEYWORD));
            assertThat(response.getBodyAsDocNode(), not(containsFieldPointedByJsonPath("hits.hits[0].fields", FIELD_AUTHOR)));
            assertThat(response.getBodyAsDocNode(), not(containsFieldPointedByJsonPath("hits.hits[0].fields", FIELD_AUTHOR_KEYWORD)));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("hits.hits[0].fields", 4));
        }
    }

    @Test
    public void shouldPerformAggregationByAuthorKeyword() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(DOCUMENT_READER_USER)) {
            HttpResponse response = client.postJson("/" + documentsIndexName + "/_search", AGGREGATION_BY_AUTHOR_QUERY);

            assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            log.info("Response should contain value of field author '{}'", response.getBody());
            String jsonPathFirstAggregationBucket = "aggregations." + AUTHOR_AGGREGATION_NAME + ".buckets[0]";
            assertThat(response.getBodyAsDocNode(), containsFieldPointedByJsonPath(jsonPathFirstAggregationBucket, "key"));
        }
    }

    @Test
    public void shouldNotPerformAggregationByAuthorKeywordWhenAccessToTheFieldIsLacking() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(DOCUMENT_TITLE_READER_USER)) {
            HttpResponse response = client.postJson("/" + documentsIndexName + "/_search", AGGREGATION_BY_AUTHOR_QUERY);

            assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            log.info("Response should NOT contain value of field author '{}'", response.getBody());
            //user DOCUMENT_TITLE_READER_USER is not allowed to access document's author field therefore aggregation backed by the author
            //field are empty
            assertThat(response.getBodyAsDocNode(), not(containsFieldPointedByJsonPath("aggregations." + AUTHOR_AGGREGATION_NAME + ".buckets[0]", "key")));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("aggregations." + AUTHOR_AGGREGATION_NAME + ".buckets", 0));
        }
    }

    @Test
    public void shouldNotPerformAggregationByAuthorKeywordWhenAccessToTheFieldIsNegated() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(DOCUMENT_READER_EXCEPT_AUTHOR_USER)) {
            HttpResponse response = client.postJson("/" + documentsIndexName + "/_search", AGGREGATION_BY_AUTHOR_QUERY);

            assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            log.info("Response should NOT contain value of field author '{}'", response.getBody());
            //user DOCUMENT_READER_EXCEPT_AUTHOR_USER is not allowed to access document's author field therefore aggregation backed by the author
            //field are empty
            assertThat(response.getBodyAsDocNode(), not(containsFieldPointedByJsonPath("aggregations." + AUTHOR_AGGREGATION_NAME + ".buckets[0]", "key")));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("aggregations." + AUTHOR_AGGREGATION_NAME + ".buckets", 0));
        }
    }

    /**
     * Instability of this test indicates cache related problems during evaluation of FLS.
     */
    @Test
    public void shouldCheckFlsForEachRequestForEachUser() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(DOCUMENT_READER_USER)) {
            HttpResponse response = client.postJson("/" + documentsIndexName + "/_search", AGGREGATION_BY_AUTHOR_QUERY);

            assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            log.info("Response should contain value of field author '{}'", response.getBody());
            String jsonPathFirstAggregationBucket = "aggregations." + AUTHOR_AGGREGATION_NAME + ".buckets[0]";
            assertThat(response.getBodyAsDocNode(), containsFieldPointedByJsonPath(jsonPathFirstAggregationBucket, "key"));
        }
        try (GenericRestClient client = cluster.getRestClient(DOCUMENT_READER_EXCEPT_AUTHOR_USER)) {

            HttpResponse response = client.postJson("/" + documentsIndexName + "/_search", AGGREGATION_BY_AUTHOR_QUERY);

            assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(response.getBodyAsDocNode(), not(containsFieldPointedByJsonPath("aggregations." + AUTHOR_AGGREGATION_NAME + ".buckets[0]", "key")));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("aggregations." + AUTHOR_AGGREGATION_NAME + ".buckets", 0));
        }
    }
}
