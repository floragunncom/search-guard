/*
 * Copyright 2023-2024 floragunn GmbH
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

import com.floragunn.fluent.collections.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Implementation based on com.floragunn.searchsupport.jobs.core.IndexJobStateStore#loadTriggerStates(java.util.Map)
 */
public class SearchScroller {

    private static final Logger log = LogManager.getLogger(SearchScroller.class);

    private final Client client;

    public SearchScroller(Client client) {
        this.client = Objects.requireNonNull(client, "Client is required");
    }


    public <T>  void scroll(SearchRequest request, TimeValue scrollTime,
        Function<SearchHit, T> resultMapper,
        Consumer<ImmutableList<T>> resultConsumer) {
        Objects.requireNonNull(request, "Search request is required to scroll");
        Objects.requireNonNull(resultMapper, "Scroll result mapper is required");
        request.scroll(scrollTime);
        try (RefCountedGuard<SearchResponse> guard = new RefCountedGuard<>()) {
            SearchResponse searchResponse = client.search(request).actionGet();
            guard.add(searchResponse);
            try {
                do {
                    SearchHit[] hits = searchResponse.getHits().getHits();
                    String scrollId = searchResponse.getScrollId();
                    if(searchResponse.getFailedShards() > 0) {
                        throw new IllegalStateException("Unexpected error during scrolling via search results.");
                    }
                    log.debug("'{}' elements were gained due to scrolling with id '{}'.", hits.length, scrollId);
                    ArrayList<T> mutableResultList = new ArrayList<>();
                    for (SearchHit searchHit : hits) {
                        mutableResultList.add(resultMapper.apply(searchHit));
                    }
                    resultConsumer.accept(ImmutableList.of(mutableResultList));
                    guard.release();
                    searchResponse = client.prepareSearchScroll(scrollId) //
                        .setScroll(scrollTime)
                        .execute() //
                        .actionGet();
                    guard.add(searchResponse);
                } while (searchResponse.getHits().getHits().length != 0);
            } finally {
                log.debug("Async clear scroll '{}'.", searchResponse.getScrollId());
                Actions.clearScrollAsync(client, searchResponse);
            }
        } catch (ElasticsearchException e) {
            log.error("Error during scrolling to load all search results", e);
            throw e;
        }
    }

}
