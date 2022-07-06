package com.floragunn.searchguard.client;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.client.methods.HttpPost;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class RestHighLevelClient implements AutoCloseable, Closeable {

    private static final JacksonJsonpMapper JACKSON_JSONP_MAPPER = new JacksonJsonpMapper();
    private final RestClientBuilder builder;
    private final RestClient restClient;
    private final ElasticsearchTransport transport;
    private final ElasticsearchClient elasticsearchClient;
    private volatile boolean closed;

    public RestHighLevelClient(RestClientBuilder builder) {
        this.builder = Objects.requireNonNull(builder);
        this.restClient = builder.build();

        this.transport = new RestClientTransport(
                restClient,
                JACKSON_JSONP_MAPPER
        );

        this.elasticsearchClient = new ElasticsearchClient(transport);
    }

    public ElasticsearchClient getJavaClient() {
        assertOpen();
        return elasticsearchClient;
    }

    @Override
    public void close() {
        closed = true;
        IOUtils.closeWhileHandlingException(transport);
    }

    public RestClient getLowLevelClient() {
        assertOpen();
        return restClient;
    }

    public Response reloadTransportCerts() throws IOException {
        assertOpen();
        return getLowLevelClient().performRequest(new Request(HttpPost.METHOD_NAME, "/_searchguard/api/ssl/transport/reloadcerts/"));
    }

    public Response reloadHttpCerts() throws IOException {
        assertOpen();
        return getLowLevelClient().performRequest(new Request(HttpPost.METHOD_NAME, "/_searchguard/api/ssl/http/reloadcerts/"));
    }

    /**
     * Search all
     * @param index
     * @return
     */
    public SearchResponse<Map> search(String index) throws IOException {
        assertOpen();
        return getJavaClient().search(new SearchRequest.Builder().index(index).build(), Map.class);
    }

    public SearchResponse<Map> search(String index, int from, int size) throws IOException {
        assertOpen();
        return getJavaClient().search(new SearchRequest.Builder().index(index).from(from).size(size).build(), Map.class);
    }

    private void assertOpen() {
        if(closed) throw new IllegalStateException("Client is already closed");
    }


    public IndexResponse index(String index, Map<String, String> source) throws IOException {
        assertOpen();
        return getJavaClient().index(i->i.index(index).document(source));
    }

    public IndexResponse index(String index, String id, Map<String, Object> source) throws IOException {
        assertOpen();
        return getJavaClient().index(i->i.index(index).id(id).document(source));
    }

    public GetResponse<Map> get(String index, String id) throws IOException {
        assertOpen();
        return getJavaClient().get(g->g.index(index).id(id), Map.class);
    }
}
