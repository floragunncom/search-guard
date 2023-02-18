package com.floragunn.searchguard.enterprise.dlsfls.legacy;

import com.floragunn.codova.documents.ContentType;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.legacy.test.RestHelper.HttpResponse;
import com.jayway.jsonpath.PathNotFoundException;
import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class FlsKeywordTest extends AbstractDlsFlsTest {

    public static final String DOCUMENTS_INDEX_NAME = "documents";
    public static final Header BASIC_CREDENTIALS_DOCUMENT_READER = encodeBasicHeader("document_reader", "admin");
    public static final Header BASIC_CREDENTIALS_DOCUMENT_TITLE_READER = encodeBasicHeader("document_title_reader", "admin");
    public static final Header BASIC_CREDENTIALS_DOCUMENT_READER_EXCEPT_AUTHOR = encodeBasicHeader("document_reader_except_author", "admin");
    public static final String FIELD_AUTHOR = "author";
    public static final String FIELD_AUTHOR_KEYWORD = "author.keyword";
    public static final String FIELD_TITLE = "title";
    public static final String FIELD_TITLE_KEYWORD = "title.keyword";
    public static final String FIELD_CONTENT = "content";
    public static final String FIELD_CONTENT_KEYWORD = "content.keyword";
    public static final String AUTHOR_AGGREGATION_NAME = "author-aggregation";
    public static final DocNode MATCH_ALL_QUERY_WITH_FIELDS = DocNode.of("query", DocNode.of("match_all", DocNode.EMPTY), "fields", asList(FIELD_TITLE, FIELD_TITLE_KEYWORD, FIELD_CONTENT, FIELD_CONTENT_KEYWORD, FIELD_AUTHOR, FIELD_AUTHOR_KEYWORD), "_source", false);
    public static final DocNode AGGREGATION_BY_AUTHOR_QUERY = DocNode.of("size", 0, "aggs", DocNode.of(AUTHOR_AGGREGATION_NAME, DocNode.of("terms", DocNode.of("field", FIELD_AUTHOR_KEYWORD))));
    public static final String AUTHOR_GOETHE = "Goethe";

    @Override
    void populateData(Client tc) {
        tc.index(new IndexRequest(DOCUMENTS_INDEX_NAME).id("0").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .source(DocNode.of(FIELD_TITLE, "document title", FIELD_CONTENT, "document content", FIELD_AUTHOR, AUTHOR_GOETHE).toJsonString(), XContentType.JSON)).actionGet();
    }

    @Test
    public void shouldReadWholeDocumentBySimpleSearch() throws Exception {
        setup();

        HttpResponse response = rh.executeGetRequest("/" + DOCUMENTS_INDEX_NAME + "/_search", BASIC_CREDENTIALS_DOCUMENT_READER);

        assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_OK));
        ContentType contentType = ContentType.parseHeader(response.getContentType());
        String author = DocNode.parse(contentType).from(response.getBody()).findSingleValueByJsonPath("hits.hits[0]._source.author", String.class);
        assertThat(author, equalTo(AUTHOR_GOETHE));
    }

    @Test(expected = PathNotFoundException.class)
    public void shouldNotContainAuthorFieldWhenUserIsNotAllowedToAccessTheField() throws Exception {
        setup();

        HttpResponse response = rh.executeGetRequest("/" + DOCUMENTS_INDEX_NAME + "/_search", BASIC_CREDENTIALS_DOCUMENT_READER_EXCEPT_AUTHOR);

        assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_OK));
        ContentType contentType = ContentType.parseHeader(response.getContentType());
        DocNode.parse(contentType).from(response.getBody()).findSingleValueByJsonPath("hits.hits[0]._source.author", String.class);
    }

    @Test
    public void shouldReadWholeDocument() throws Exception {
        setup();
        String query = MATCH_ALL_QUERY_WITH_FIELDS.toJsonString();

        HttpResponse response = rh.executePostRequest("/" + DOCUMENTS_INDEX_NAME +"/_search?pretty", query,
            BASIC_CREDENTIALS_DOCUMENT_READER);

        assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_OK));
        ContentType contentType = ContentType.parseHeader(response.getContentType());
        List<DocNode> fields = DocNode.parse(contentType).from(response.getBody()).findNodesByJsonPath("hits.hits[0].fields");
        assertThat(fields, hasSize(1));
        DocNode fieldsNode = fields.get(0);
        assertThat(fieldsNode.containsKey(FIELD_TITLE), equalTo(true));
        assertThat(fieldsNode.containsKey(FIELD_TITLE_KEYWORD), equalTo(true));
        assertThat(fieldsNode.containsKey(FIELD_CONTENT), equalTo(true));
        assertThat(fieldsNode.containsKey(FIELD_CONTENT_KEYWORD), equalTo(true));
        assertThat(fieldsNode.containsKey(FIELD_AUTHOR), equalTo(true));
        assertThat(fieldsNode.containsKey(FIELD_AUTHOR_KEYWORD), equalTo(true));
        assertThat(fieldsNode.size(), equalTo(6));
    }

    @Test
    public void shouldReadOnlyTitleFromDocument() throws Exception {
        setup();
        String query = MATCH_ALL_QUERY_WITH_FIELDS.toJsonString();

        HttpResponse response = rh.executePostRequest("/" + DOCUMENTS_INDEX_NAME +"/_search?pretty", query,
            BASIC_CREDENTIALS_DOCUMENT_TITLE_READER);

        assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_OK));
        ContentType contentType = ContentType.parseHeader(response.getContentType());
        List<DocNode> fields = DocNode.parse(contentType).from(response.getBody()).findNodesByJsonPath("hits.hits[0].fields");
        assertThat(fields, hasSize(1));
        DocNode fieldsNode = fields.get(0);
        assertThat(fieldsNode.containsKey(FIELD_TITLE), equalTo(true));
        assertThat(fieldsNode.containsKey(FIELD_TITLE_KEYWORD), equalTo(true));
        assertThat(fieldsNode.containsKey(FIELD_CONTENT), equalTo(false));
        assertThat(fieldsNode.containsKey(FIELD_CONTENT_KEYWORD), equalTo(false));
        assertThat(fieldsNode.size(), equalTo(2));
    }

    @Test
    public void shouldReadEverythingBesidesAuthor() throws Exception {
        setup();
        String query = MATCH_ALL_QUERY_WITH_FIELDS.toJsonString();

        HttpResponse response = rh.executePostRequest("/" + DOCUMENTS_INDEX_NAME +"/_search?pretty", query,
            BASIC_CREDENTIALS_DOCUMENT_READER_EXCEPT_AUTHOR);

        assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_OK));
        ContentType contentType = ContentType.parseHeader(response.getContentType());
        List<DocNode> fields = DocNode.parse(contentType).from(response.getBody()).findNodesByJsonPath("hits.hits[0].fields");
        assertThat(fields, hasSize(1));
        DocNode fieldsNode = fields.get(0);
        assertThat(fieldsNode.containsKey(FIELD_TITLE), equalTo(true));
        assertThat(fieldsNode.containsKey(FIELD_TITLE_KEYWORD), equalTo(true));
        assertThat(fieldsNode.containsKey(FIELD_CONTENT), equalTo(true));
        assertThat(fieldsNode.containsKey(FIELD_CONTENT_KEYWORD), equalTo(true));
        assertThat(fieldsNode.containsKey(FIELD_AUTHOR), equalTo(false));
        assertThat(fieldsNode.containsKey(FIELD_AUTHOR_KEYWORD), equalTo(false));
        assertThat(fieldsNode.size(), equalTo(4));
    }

    @Test
    public void shouldPerformAggregationByAuthorKeyword() throws Exception {
        setup();
        String query = AGGREGATION_BY_AUTHOR_QUERY.toJsonString();

        HttpResponse response = rh.executePostRequest("/" + DOCUMENTS_INDEX_NAME +"/_search?pretty", query,
            BASIC_CREDENTIALS_DOCUMENT_READER);

        assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_OK));
        ContentType contentType = ContentType.parseHeader(response.getContentType());
        List<DocNode> bucketsList = DocNode.parse(contentType).from(response.getBody()).findNodesByJsonPath("aggregations." + AUTHOR_AGGREGATION_NAME + ".buckets[0]");
        assertThat(bucketsList, hasSize(1));
        DocNode buckets = bucketsList.get(0);
        assertThat(buckets.containsKey("key"), equalTo(true));
    }

    @Test
    public void shouldNotPerformAggregationByAuthorKeywordWhenAccessToTheFieldIsLacking() throws Exception {
        setup();
        String query = AGGREGATION_BY_AUTHOR_QUERY.toJsonString();

        HttpResponse response = rh.executePostRequest("/" + DOCUMENTS_INDEX_NAME +"/_search?pretty", query,
            BASIC_CREDENTIALS_DOCUMENT_TITLE_READER);

        assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_OK));
        ContentType contentType = ContentType.parseHeader(response.getContentType());
        List<DocNode> bucketsNodeList = DocNode.parse(contentType).from(response.getBody()).findNodesByJsonPath("aggregations." + AUTHOR_AGGREGATION_NAME + ".buckets");
        assertThat(bucketsNodeList, hasSize(1));
        DocNode buckets = bucketsNodeList.get(0);
        assertThat(buckets.size(), equalTo(0));
    }
}
