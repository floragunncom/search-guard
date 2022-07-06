/*
 * Copyright 2021 floragunn GmbH
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

package com.floragunn.signals.util;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.TopHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.floragunn.signals.watch.result.WatchLog;

public class WatchLogSearch {
    private static final Logger log = LogManager.getLogger(WatchLogSearch.class);

    private Client client;
    private String watchId;
    private Long watchVersion;
    private String tenantName = "_main";
    private String index = null;
    private int count = 1;
    private SortOrder searchSortOrder = SortOrder.DESC;
    private SortOrder resultSortOrder = SortOrder.ASC;
    private Duration timeout = Duration.ofSeconds(10);

    public WatchLogSearch(Client client) {
        this.client = client;
    }

    public WatchLogSearch watchId(String watchId) {
        this.watchId = watchId;
        return this;
    }

    public WatchLogSearch watchVersion(long watchVersion) {
        this.watchVersion = watchVersion;
        return this;
    }

    public WatchLogSearch tenant(String tenant) {
        this.tenantName = tenant;
        return this;
    }

    public WatchLogSearch index(String index) {
        this.index = index;
        return this;
    }

    public WatchLogSearch count(int count) {
        this.count = count;
        return this;
    }

    public WatchLogSearch toTheEnd() {
        searchSortOrder = SortOrder.DESC;
        resultSortOrder = SortOrder.ASC;
        return this;
    }

    public WatchLogSearch fromTheStart() {
        searchSortOrder = SortOrder.ASC;
        resultSortOrder = SortOrder.ASC;
        return this;
    }

    public WatchLogSearch timeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public List<WatchLog> peek() {
        try {

            BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();

            if (watchId != null) {
                queryBuilder = queryBuilder.must(new TermQueryBuilder("watch_id", watchId));
            }

            if (watchVersion != null) {
                queryBuilder = queryBuilder.must(new TermQueryBuilder("watch_version", watchVersion));
            }

            if (tenantName != null) {
                queryBuilder = queryBuilder.must(new TermQueryBuilder("tenant", tenantName));
            }

            SearchResponse searchResponse = client
                    .search(new SearchRequest(index)
                            .source(new SearchSourceBuilder().size(0).query(queryBuilder).sort("execution_end", resultSortOrder)
                                    .aggregation(AggregationBuilders.topHits("execution_end").sort("execution_end", searchSortOrder).size(count))))
                    .actionGet();

            if (searchResponse.getAggregations() == null) {
                return Collections.emptyList();
            }

            TopHits topHits = (TopHits) searchResponse.getAggregations().get("execution_end");

            ArrayList<WatchLog> result = new ArrayList<>(count);

            if (topHits != null) {
                for (SearchHit searchHit : topHits.getHits().getHits()) {
                    result.add(WatchLog.parse(searchHit.getId(), searchHit.getSourceAsString()));
                }
            }

            return result;
        } catch (org.elasticsearch.index.IndexNotFoundException | SearchPhaseExecutionException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Error in getMostRecenWatchLog(" + tenantName + ", " + watchId + ")", e);
        }
    }

    public WatchLog awaitSingle() {
        List<WatchLog> result = await(timeout);

        if (result.size() > 0) {
            return result.get(0);
        } else {
            return null;
        }
    }

    public List<WatchLog> await() {
        return await(timeout);
    }

    public List<WatchLog> await(Duration timeout) {
        try {
            long start = System.currentTimeMillis();
            long end = start + timeout.toMillis();
            Exception indexNotFoundException = null;
            long lastDebugLog = start;

            while (System.currentTimeMillis() < end) {
                try {

                    List<WatchLog> watchLogs = peek();

                    if (watchLogs.size() == count) {
                        log.info("Found " + watchLogs + " for " + watchId + " after " + (System.currentTimeMillis() - start) + " ms");

                        return watchLogs;
                    } else if (lastDebugLog < System.currentTimeMillis() - 1000) {
                        log.debug("Still waiting for watch logs; found so far: " + watchLogs);

                        lastDebugLog = System.currentTimeMillis();
                    }

                    indexNotFoundException = null;

                } catch (org.elasticsearch.index.IndexNotFoundException | SearchPhaseExecutionException e) {
                    indexNotFoundException = e;
                }

                Thread.sleep(10);

            }

            if (indexNotFoundException != null) {
                log.warn("Did not find watch log index for " + watchId + " after " + (System.currentTimeMillis() - start) + " ms: ",
                        indexNotFoundException);
            } else {
                log.warn("Did not find watch log for " + watchId + " after " + (System.currentTimeMillis() - start) + " ms\n\n" + peek());
            }
            return Collections.emptyList();
        } catch (Exception e) {
            log.error("Exception in awaitWatchLog for " + watchId + ")", e);
            throw new RuntimeException("Exception in awaitWatchLog for " + watchId + ")", e);
        }
    }
}
