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
package com.floragunn.signals.watch.result;

import com.floragunn.searchguard.internalauthtoken.InternalAuthTokenProvider;
import com.floragunn.signals.settings.SignalsSettings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

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
    private final ToXContent.Params toXparams;
    private final RefreshPolicy refreshPolicy;
    private final boolean syncIndexing;

    public WatchLogIndexWriter(Client client, String tenant, SignalsSettings settings, ToXContent.Params toXparams) {
        this.client = client;
        this.tenant = tenant;
        this.settings = settings;
        this.toXparams = toXparams;
        this.refreshPolicy = settings.getStaticSettings().getWatchLogRefreshPolicy();
        this.syncIndexing = settings.getStaticSettings().isWatchLogSyncIndexingEnabled();
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

            if (log.isDebugEnabled()) {
                log.debug("Going to write WatchLog " + (refreshPolicy == RefreshPolicy.IMMEDIATE ? " (immediate) " : "") + watchLog);
            }

            // Elevate permissions
            threadContext.putHeader(InternalAuthTokenProvider.TOKEN_HEADER, null);
            threadContext.putHeader(InternalAuthTokenProvider.AUDIENCE_HEADER, null);

            watchLog.toXContent(jsonBuilder, toXparams);
            indexRequest.source(jsonBuilder);
            indexRequest.setRefreshPolicy(refreshPolicy);

            if (syncIndexing) {
                IndexResponse response = client.index(indexRequest).actionGet();

                if (log.isDebugEnabled()) {
                    log.debug("Completed sync writing WatchLog: " + watchLog + "\n" + Strings.toString(response));
                }
            } else {
                client.index(indexRequest, new ActionListener<IndexResponse>() {

                    @Override
                    public void onResponse(IndexResponse response) {
                        if (log.isDebugEnabled()) {
                            log.debug("Completed writing WatchLog: " + watchLog + "\n" + Strings.toString(response));
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        log.error("Error while writing WatchLog " + watchLog, e);
                    }
                });
            }

        } catch (Exception e) {
            log.error("Error while writing WatchLog " + watchLog, e);
        }

    }

    public static WatchLogIndexWriter forTenant(Client client, String tenantName, SignalsSettings settings, ToXContent.Params toXparams) {
        return new WatchLogIndexWriter(client, tenantName, settings, toXparams);
    }
}
