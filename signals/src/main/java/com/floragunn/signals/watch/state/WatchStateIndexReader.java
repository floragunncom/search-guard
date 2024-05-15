/*
 * Copyright 2020-2021 floragunn GmbH
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

package com.floragunn.signals.watch.state;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.floragunn.searchsupport.client.RefCountedGuard;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.searchsupport.client.Actions;

public class WatchStateIndexReader {
    private static final Logger log = LogManager.getLogger(WatchStateIndexReader.class);

    private final String tenant;
    private final String indexName;
    private final String watchIdPrefix;
    private final Client client;

    public WatchStateIndexReader(String tenant, String watchIdPrefix, String indexName, Client client) {
        this.tenant = tenant;
        this.watchIdPrefix = watchIdPrefix;
        this.indexName = indexName;
        this.client = client;
    }

    public WatchState get(String watchId) throws IOException, DocumentParseException {
        String prefixedId = watchIdPrefix + watchId;

        GetResponse getResponse = client.prepareGet().setIndex(this.indexName).setId(prefixedId).get();

        if (getResponse.isExists()) {
            return WatchState.createFromJson(tenant, getResponse.getSourceAsString());
        } else {
            throw new IOException("State of " + watchId + " does not exist: " + getResponse);
        }
    }

    public Map<String, WatchState> get(Collection<String> watchIds) {
        try {
            if (watchIds.isEmpty()) {
                return Collections.emptyMap();
            }

            Map<String, WatchState> result = new HashMap<>(watchIds.size());

            QueryBuilder queryBuilder = QueryBuilders.idsQuery()
                    .addIds(watchIds.stream().map((watchId) -> watchIdPrefix + watchId).toArray(String[]::new));

            if (log.isDebugEnabled()) {
                log.debug("Going to do query: " + queryBuilder);
            }

            SearchResponse searchResponse = client.prepareSearch(this.indexName).setQuery(queryBuilder).setSize(1000).setScroll(new TimeValue(10000))
                    .get();

            try (RefCountedGuard<SearchResponse> guard = new RefCountedGuard<>()){
                guard.add(searchResponse);
                do {
                    for (SearchHit searchHit : searchResponse.getHits().getHits()) {
                        try {
                            result.put(searchHit.getId().substring(watchIdPrefix.length()),
                                    WatchState.createFromJson(tenant, searchHit.getSourceAsString()));
                        } catch (Exception e) {
                            log.error("Error while loading " + searchHit, e);
                        }
                    }
                    String scrollId = searchResponse.getScrollId();
                    guard.release();
                    searchResponse = client.prepareSearchScroll(scrollId).setScroll(new TimeValue(10000)).execute().actionGet();
                    guard.add(searchResponse);
                } while (searchResponse.getHits().getHits().length != 0);

                if (log.isDebugEnabled()) {
                    log.debug("Got states: " + result);
                }
            } finally {
                Actions.clearScrollAsync(client, searchResponse);
            }

            return result;
        } catch (ElasticsearchException e) {
            log.error("Error in WatchStateIndexReader.get()", e);
            throw e;
        }
    }
}
