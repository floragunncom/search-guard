/*
 * Copyright 2023 floragunn GmbH
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public class SearchScroller {

    private static final Logger log = LogManager.getLogger(SearchScroller.class);

    private final Client client;

    public SearchScroller(Client client) {
        this.client = Objects.requireNonNull(client, "Client is required");
    }

    public <T> List<T> scroll(SearchRequest request, TimeValue scrollTimeout, Function<SearchHit, T> resultMapper) {
        Objects.requireNonNull(request, "Search request is required to scroll");
        Objects.requireNonNull(resultMapper, "Scroll result mapper is required");
        Objects.requireNonNull(scrollTimeout, "Scroll timeout is required");
        request.scroll(scrollTimeout);
        try {
            SearchResponse searchResponse = client.search(request).actionGet();
            try {
                List<T> results = new ArrayList<>();
                do {
                    SearchHit[] hits = searchResponse.getHits().getHits();
                    String scrollId = searchResponse.getScrollId();
                    if(searchResponse.getFailedShards() > 0) {
                        throw new IllegalStateException("Unexpected error during scrolling via search results.");
                    }
                    log.debug("'{}' elements were gained due to scrolling with id '{}'.", hits.length, scrollId);
                    for (SearchHit searchHit : hits) {
                        results.add(resultMapper.apply(searchHit));
                    }
                    searchResponse = client.prepareSearchScroll(searchResponse.getScrollId())
                            .setScroll(scrollTimeout)
                        .execute()
                        .actionGet();
                } while (searchResponse.getHits().getHits().length != 0);

                return results;
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
