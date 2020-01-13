package com.floragunn.signals.watch.state;

import java.io.IOException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class WatchStateIndexWriter implements WatchStateWriter {
    private static final Logger log = LogManager.getLogger(WatchStateIndexWriter.class);

    private final String indexName;
    private final String watchIdPrefix;
    private final Client client;

    public WatchStateIndexWriter(String watchIdPrefix, String indexName, Client client) {
        this.watchIdPrefix = watchIdPrefix;
        this.indexName = indexName;
        this.client = client;
    }

    public void put(String watchId, WatchState watchState) {
        
        try {
            IndexRequest indexRequest = createIndexRequest(watchId, watchState, RefreshPolicy.IMMEDIATE);

            client.index(indexRequest, new ActionListener<IndexResponse>() {

                @Override
                public void onResponse(IndexResponse response) {
                    if (log.isDebugEnabled()) {
                        log.debug("Updated " + watchId + " to:\n" + watchState + "\n" + Strings.toString(response));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    log.error("Error while writing WatchState " + watchState, e);
                }
            });

        } catch (Exception e) {
            log.error("Error while writing WatchState " + watchState, e);
        }
    }

    public void putAll(Map<String, WatchState> idToStateMap) {
        BulkRequest bulkRequest = new BulkRequest();

        for (Map.Entry<String, WatchState> entry : idToStateMap.entrySet()) {
            try {
                bulkRequest.add(createIndexRequest(entry.getKey(), entry.getValue(), RefreshPolicy.NONE));
            } catch (Exception e) {
                log.error("Error while serializing " + entry);
            }
        }

        client.bulk(bulkRequest, new ActionListener<BulkResponse>() {

            @Override
            public void onResponse(BulkResponse response) {
                if (log.isDebugEnabled()) {
                    log.debug("Updated " + idToStateMap.keySet() + "\n" + Strings.toString(response));
                }
            }

            @Override
            public void onFailure(Exception e) {
                log.error("Error while writing WatchState " + idToStateMap, e);
            }
        });
    }

    private IndexRequest createIndexRequest(String watchId, WatchState watchState, RefreshPolicy refreshPolicy) throws IOException {
        try (XContentBuilder jsonBuilder = XContentFactory.jsonBuilder()) {
            IndexRequest indexRequest = new IndexRequest(indexName).id(watchIdPrefix + watchId);
            watchState.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
            indexRequest.source(jsonBuilder);
            indexRequest.setRefreshPolicy(refreshPolicy);

            return indexRequest;
        }
    }
}
