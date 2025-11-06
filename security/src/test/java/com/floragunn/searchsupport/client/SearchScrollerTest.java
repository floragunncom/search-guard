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

package com.floragunn.searchsupport.client;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsOnlyFields;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

/**
 * SearchScroller is located in the support module, but we need to test it here in order to prevent cyclic dependencies
 */
public class SearchScrollerTest {

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder()
            .singleNode()
            .sslEnabled()
            .embedded()
            .build();


    private final SearchScroller searchScroller = new SearchScroller(cluster.getInternalNodeClient());

    @Test(expected = IndexNotFoundException.class)
    public void scroll_notExistingIndex_shouldFail() {
        String testIndex = "index-does-not-exist";
        searchScroller.scroll(new SearchRequest(testIndex), TimeValue.ONE_MINUTE, Function.identity(), list -> {});
    }

    @Test
    public void scroll_emptyIndex_shouldReturnNoData() throws Exception {
        String testIndex = "index-empty";
        try (GenericRestClient client = cluster.getAdminCertRestClient().trackResources()) {

            GenericRestClient.HttpResponse response = client.put(testIndex);
            assertThat(response, isOk());

            List<SearchHit> results = new ArrayList<>();
            searchScroller.scroll(new SearchRequest(testIndex), TimeValue.ONE_MINUTE, Function.identity(), results::addAll);
            assertThat(results, empty());
        }
    }

    @Test
    public void scroll_lessDocsThanSearchSize_shouldReturnData() throws Exception {
        String testIndex = "index-lessDocsThanSearchSize".toLowerCase(Locale.ROOT);
        int numberOfDocs = 6;
        try (GenericRestClient client = cluster.getAdminCertRestClient().trackResources()) {

            GenericRestClient.HttpResponse response = client.put(testIndex);
            assertThat(response, isOk());

            saveDocs(testIndex, numberOfDocs);

            List<DocNode> results = new ArrayList<>();
            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
                    .size(numberOfDocs * 4)
                    .sort("number", SortOrder.ASC);
            Function<SearchHit, DocNode> mapper = hit -> DocNode.wrap(hit.getSourceAsMap());

            searchScroller.scroll(new SearchRequest(testIndex).source(searchSourceBuilder), TimeValue.ONE_MINUTE, mapper, results::addAll);
            assertThat(results, hasSize(numberOfDocs));
            for (int i = 0; i < numberOfDocs; i++) {
                assertThat(results.get(i).toJsonString(), results.get(i), containsOnlyFields("$", asList("fieldA", "number")));
                assertThat(results.get(i).toJsonString(), results.get(i), containsValue("$.fieldA", "a" + i));
                assertThat(results.get(i).toJsonString(), results.get(i), containsValue("$.number", i));
            }
        }
    }

    @Test
    public void scroll_moreDocsThanSearchSize_shouldReturnData() throws Exception {
        String testIndex = "index-moreDocsThanSearchSize".toLowerCase(Locale.ROOT);
        int numberOfDocs = 150;
        try (GenericRestClient client = cluster.getAdminCertRestClient().trackResources()) {

            GenericRestClient.HttpResponse response = client.put(testIndex);
            assertThat(response, isOk());

            saveDocs(testIndex, numberOfDocs);

            List<DocNode> results = new ArrayList<>();
            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
                    .size(numberOfDocs / 30)
                    .sort("number", SortOrder.ASC);
            Function<SearchHit, DocNode> mapper = hit -> DocNode.wrap(hit.getSourceAsMap()).without("number");

            searchScroller.scroll(new SearchRequest(testIndex).source(searchSourceBuilder), TimeValue.ONE_MINUTE, mapper, results::addAll);
            assertThat(results, hasSize(numberOfDocs));
            for (int i = 0; i < numberOfDocs; i++) {
                assertThat(results.get(i).toJsonString(), results.get(i), containsOnlyFields("$", singleton("fieldA")));
                assertThat(results.get(i).toJsonString(), results.get(i), containsValue("$.fieldA", "a" + i));
            }
        }
    }

    private void saveDocs(String index, int numberOfDocs) throws Exception {
        String bulkBody = IntStream.range(0, numberOfDocs).boxed()
                .map(docNo -> {
                    StringBuilder sb = new StringBuilder();
                    sb.append(String.format("{ \"index\" : { \"_index\" : \"%s\" } }", index));
                    sb.append("\n");
                    sb.append(String.format("{ \"fieldA\" : \"a%d\", \"number\" : %d }", docNo, docNo));
                    return sb.toString();
                })
                .collect(Collectors.joining("\n")).concat("\n");

        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse response = client.postJson("/_bulk?refresh=true", bulkBody);
            assertThat(response, isOk());
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.errors", false));
        }
    }
}
