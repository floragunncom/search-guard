/*
 * Copyright 2019-2023 floragunn GmbH
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
package com.floragunn.signals.actions.watch.generic.rest;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.WatchBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.concurrent.ExecutionException;

import static org.apache.http.HttpStatus.SC_CREATED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

abstract class AbstractGenericWatchTest {

    private static final Logger log = LogManager.getLogger(GenericWatchTest.class);

    protected static final String DEFAULT_TENANT = "_main";
    protected static final String INDEX_SOURCE = "test_source_index";
    protected static final String CRON_ALMOST_NEVER = "0 0 0 1 1 ?";

    protected abstract GenericRestClient getAdminRestClient();

    protected String createGenericWatch(String tenant, String watchId, String...parameterNames) throws Exception {
        return createWatch(tenant, watchId, true, parameterNames);
    }

    protected String createWatch(String tenant, String watchId, boolean generic, String...parameterNames) throws Exception {
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        try (GenericRestClient restClient = getAdminRestClient()) {
            Watch watch = new WatchBuilder(watchId).instances(generic, parameterNames).cronTrigger(CRON_ALMOST_NEVER)//
                .search(INDEX_SOURCE).query("{\"match_all\" : {} }").as("testsearch")//
                .then().index("testsink").throttledFor("1h").name("testsink").build();
            String watchJson = watch.toJson();
            log.debug("Create watch '{}' with id '{}'.", watchJson, watchId);
            GenericRestClient.HttpResponse response = restClient.putJson(watchPath, watchJson);
            log.debug("Create watch '{}' response status '{}' and body '{}'", watchId, response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            return watchPath;
        }
    }

    protected long countDocumentInIndex(Client client, String index) throws InterruptedException, ExecutionException {
        SearchResponse response = findAllDocuments(client, index);
        long count = response.getHits().getTotalHits().value;
        log.debug("Number of documents in index '{}' is '{}'", index, count);
        return count;
    }

    protected SearchResponse findAllDocuments(Client client, String index) throws InterruptedException, ExecutionException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        return findDocuments(client, index, searchSourceBuilder);
    }

    protected SearchResponse findDocuments(Client client, String index, SearchSourceBuilder searchSourceBuilder)
        throws InterruptedException, ExecutionException {
        SearchRequest request = new SearchRequest(index);
        request.source(searchSourceBuilder);
        return client.search(request).get();
    }

    protected long countDocumentWithTerm(Client client, String index, String fieldName, String fieldValue)
        throws ExecutionException, InterruptedException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery(fieldName, fieldValue));
        SearchResponse response = findDocuments(client, index, searchSourceBuilder);
        log.debug("Search document with term '{}' value '{}' is '{}'.", fieldName, fieldValue, response);
        long count = response.getHits().getTotalHits().value;
        log.debug("Number of documents with term '{}' and value '{}' is '{}'.", fieldName, fieldValue, count);
        return count;
    }

    protected String allInstancesPath(String watchId) {
        return String.format("/_signals/watch/%s/%s/instances", DEFAULT_TENANT, watchId);
    }

    protected String instancePath(String watchId, String instanceId) {
        return String.format("/_signals/watch/%s/%s/instances/%s", DEFAULT_TENANT, watchId, instanceId);
    }

    protected String watchPath(String watchId) {
        return "/_signals/watch/" + DEFAULT_TENANT + "/" + watchId;
    }

    protected SearchHit[] findAllDocumentSearchHits(Client client, String index) throws ExecutionException, InterruptedException {
        SearchResponse response = findAllDocuments(client, index);
        return response.getHits().getHits();
    }
}
