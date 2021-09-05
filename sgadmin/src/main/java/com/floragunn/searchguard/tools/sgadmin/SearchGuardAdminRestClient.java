package com.floragunn.searchguard.tools.sgadmin;

import java.io.IOException;
import java.util.Collections;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.Validatable;
import org.elasticsearch.core.CheckedFunction;

import com.fasterxml.jackson.databind.ObjectMapper;

public class SearchGuardAdminRestClient extends RestHighLevelClient {

    public SearchGuardAdminRestClient(RestClientBuilder restClientBuilder) {
        super(restClientBuilder);
    }

    public GenericResponse reloadHttpCerts() throws ElasticsearchStatusException, IOException {
        return performRequest(new EmptyRequest(), r -> new Request(HttpPost.METHOD_NAME, "/_searchguard/api/ssl/http/reloadcerts/"),
                RequestOptions.DEFAULT, ResponseConverters.generic, Collections.emptySet());
    }

    public GenericResponse reloadTransportCerts() throws ElasticsearchStatusException, IOException {
        return performRequest(new EmptyRequest(), r -> new Request(HttpPost.METHOD_NAME, "/_searchguard/api/ssl/transport/reloadcerts/"),
                RequestOptions.DEFAULT, ResponseConverters.generic, Collections.emptySet());
    }

    private static class ResponseConverters {
        private static final ObjectMapper MAPPER = new ObjectMapper();

        static CheckedFunction<Response, GenericResponse, IOException> generic = read(GenericResponse.class);

        private static <Entity> CheckedFunction<Response, Entity, IOException> read(Class<Entity> clazz) {
            return r -> MAPPER.readValue(r.getEntity().getContent(), clazz);
        }
    }

    public static class EmptyRequest implements Validatable {

    }

    public static class GenericResponse {
        private String message;
        private String errror;

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public String getErrror() {
            return errror;
        }

        public void setErrror(String errror) {
            this.errror = errror;
        }

    }
}
