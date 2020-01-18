package com.floragunn.signals.watch.result;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.floragunn.searchguard.internalauthtoken.InternalAuthTokenProvider;
import com.floragunn.signals.settings.SignalsSettings;

/**
 * TODO maybe integrate this with scheduler framework? We won't get logs right now if something
 * in the scheduler goes wrong
 *
 */
public class WatchLogIndexWriter implements WatchLogWriter {
    private static final Logger log = LogManager.getLogger(WatchLogIndexWriter.class);

    private final Client client;
    private final String tenant;
    private final SignalsSettings settings;

    public WatchLogIndexWriter(Client client, String tenant, SignalsSettings settings) {
        this.client = client;
        this.tenant = tenant;
        this.settings = settings;
    }

    @Override
    public void put(WatchLog watchLog) {
        String indexName = settings.getDynamicSettings().getWatchLogIndex();

        IndexRequest indexRequest = new IndexRequest(indexName);
        ThreadContext threadContext = client.threadPool().getThreadContext();

        try (XContentBuilder jsonBuilder = XContentFactory.jsonBuilder(); StoredContext storedContext = threadContext.stashContext()) {

            if (watchLog.getTenant() == null) {
                watchLog.setTenant(tenant);
            }

            // Elevate permissions
            threadContext.putHeader(InternalAuthTokenProvider.TOKEN_HEADER, null);
            threadContext.putHeader(InternalAuthTokenProvider.AUDIENCE_HEADER, null);

            watchLog.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
            indexRequest.source(jsonBuilder);

            client.index(indexRequest, new ActionListener<IndexResponse>() {

                @Override
                public void onResponse(IndexResponse response) {

                }

                @Override
                public void onFailure(Exception e) {
                    log.error("Error while writing WatchLog " + watchLog, e);
                }
            });

        } catch (Exception e) {
            log.error("Error while writing WatchLog " + watchLog, e);
        }

    }

    public static WatchLogIndexWriter forTenant(Client client, String tenantName, SignalsSettings settings) {
        return new WatchLogIndexWriter(client, tenantName, settings);
    }

}
