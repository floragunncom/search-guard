package com.floragunn.signals.watch.state;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

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

    public WatchState get(String watchId) throws IOException {
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

            do {
                for (SearchHit searchHit : searchResponse.getHits().getHits()) {
                    try {
                        result.put(searchHit.getId().substring(watchIdPrefix.length()),
                                WatchState.createFromJson(tenant, searchHit.getSourceAsString()));
                    } catch (Exception e) {
                        log.error("Error while loading " + searchHit, e);
                    }
                }
                searchResponse = client.prepareSearchScroll(searchResponse.getScrollId()).setScroll(new TimeValue(10000)).execute().actionGet();

            } while (searchResponse.getHits().getHits().length != 0);

            if (log.isDebugEnabled()) {
                log.debug("Got states: " + result);
            }

            return result;
        } catch (ElasticsearchException e) {
            log.error("Error in WatchStateIndexReader.get()", e);
            throw e;
        }
    }
}
