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

package com.floragunn.searchguard.authc.session.backend;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.Scheduler.Cancellable;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.searchguard.modules.state.ComponentState;
import com.floragunn.searchguard.modules.state.ComponentState.State;
import com.floragunn.searchguard.support.PrivilegedConfigClient;

public class SessionActivityTracker {
    private static final Logger log = LogManager.getLogger(SessionActivityTracker.class);

    private final String indexName;
    private final SessionService sessionService;
    private final PrivilegedConfigClient privilegedConfigClient;
    private final Map<String, Long> lastAccess = new ConcurrentHashMap<>();
    private final ThreadPool threadPool;
    private Duration minFlushInterval = Duration.ofSeconds(30);
    private Duration inactivityTimeout;
    private long inactivityBeforeExpiryMillis;
    private TimeValue flushInterval;
    private Cancellable job;
    private Random random;
    private final ComponentState componentState = new ComponentState(1, null, "session_activity_tracker", SessionActivityTracker.class);
    private volatile long lastComponentStateUpdate = -1;

    public SessionActivityTracker(Duration inactivityTimeout, SessionService sessionService, String indexName,
            PrivilegedConfigClient privilegedConfigClient, ThreadPool threadPool) {
        this.indexName = indexName;
        this.sessionService = sessionService;
        this.privilegedConfigClient = privilegedConfigClient;
        this.threadPool = threadPool;
        this.random = new Random(System.currentTimeMillis() + hashCode());

        setInactivityTimeout(inactivityTimeout);
    }

    public void trackAccess(SessionToken authToken) {
        long now = System.currentTimeMillis();
                
        lastAccess.put(authToken.getId(), now + inactivityBeforeExpiryMillis);

        if (lastComponentStateUpdate + 10 * 1000 < now) {
            lastComponentStateUpdate = now;
            componentState.addDetail("Sessions: " + lastAccess.size());
        }

    }

    public void checkExpiryAndTrackAccess(SessionToken authToken, Consumer<Boolean> onResult, Consumer<Exception> onFailure) {
        Instant now = Instant.now();

        if (authToken.getDynamicExpiryTime().isAfter(now)) {
            // all good
            if (log.isTraceEnabled()) {
                log.trace("Checked timeout of " + authToken.getId() + "; token is not timed out. Expiry: " + authToken.getDynamicExpiryTime());
            }
            trackAccess(authToken);
            onResult.accept(Boolean.TRUE);
            return;
        }

        // The token seems to have expired. However, the value we got could be from the cache which is not up-to-date.

        Long dynamicExpiryTime = lastAccess.get(authToken.getId());

        if (dynamicExpiryTime != null && dynamicExpiryTime.longValue() > now.toEpochMilli()) {
            // all good
            if (log.isTraceEnabled()) {
                log.trace("Checked timeout of " + authToken.getId() + "; token is not timed out (needed cache recheck). Expiry: "
                        + authToken.getDynamicExpiryTime());
            }

            trackAccess(authToken);
            onResult.accept(Boolean.TRUE);
            return;
        }

        //  Now do a double-check by reading the token from the index.

        if (log.isTraceEnabled()) {
            log.trace("The token " + authToken.getId() + " seems to have expired; re-checking with fresh index data");
        }

        sessionService.getByIdFromIndex(authToken.getId(), (refreshedAuthToken) -> {
            if (refreshedAuthToken.getDynamicExpiryTime().isAfter(now)) {
                // all good

                if (log.isTraceEnabled()) {
                    log.trace("Checked timeout of " + authToken.getId() + "; token is not timed out (needed index recheck). Expiry: "
                            + authToken.getDynamicExpiryTime());
                }

                trackAccess(authToken);
                onResult.accept(Boolean.TRUE);
            } else {
                if (log.isInfoEnabled()) {
                    log.info("The auth token " + authToken.getId() + " is expired. Expiry: " + refreshedAuthToken.getDynamicExpiryTime());
                }

                onResult.accept(Boolean.FALSE);
            }

        }, (noSuchAuthTokenException) -> onResult.accept(Boolean.FALSE), onFailure);

    }

    public Duration getInactivityTimeout() {
        return inactivityTimeout;
    }

    public void setInactivityTimeout(Duration inactivityBeforeExpiry) {
        if (this.inactivityTimeout != null && inactivityBeforeExpiry.equals(this.inactivityTimeout)) {
            return;
        }

        this.inactivityTimeout = inactivityBeforeExpiry;
        this.inactivityBeforeExpiryMillis = inactivityBeforeExpiry.toMillis();

        Duration newFlushInterval = inactivityBeforeExpiry.minusSeconds(120);

        if (newFlushInterval.compareTo(minFlushInterval) < 0) {
            newFlushInterval = minFlushInterval;
        }

        this.flushInterval = TimeValue.timeValueMillis(newFlushInterval.toMillis());

        schedule();
    }

    private void schedule() {
        if (job != null) {
            job.cancel();
        }

        job = threadPool.scheduleWithFixedDelay(() -> flushWithJitter(1), flushInterval, ThreadPool.Names.GENERIC);
    }

    private void flushWithJitter(int fixedDelay) {
        if (!lastAccess.isEmpty()) {
            threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueSeconds(random.nextInt(20) + fixedDelay), ThreadPool.Names.GENERIC,
                    () -> this.flush());
        }
    }

    private void flush() {
        Map<String, Long> lastAccessCopy = new HashMap<>(lastAccess);

        if (log.isDebugEnabled()) {
            log.debug("Flushing " + lastAccessCopy.size() + " dynamic_expires entries");
        }

        if (log.isTraceEnabled()) {
            log.trace(lastAccessCopy);
        }

        // Note: identity comparison (==) is okay here because entries won't have an equal time with different identities
        lastAccess.entrySet().removeIf((entry) -> lastAccessCopy.get(entry.getKey()) == entry.getValue());

        BoolQueryBuilder query = new BoolQueryBuilder();
        query.minimumShouldMatch(1);

        for (Map.Entry<String, Long> entry : lastAccessCopy.entrySet()) {
            query.should(new BoolQueryBuilder().must(QueryBuilders.idsQuery().addIds(entry.getKey()))
                    .must(QueryBuilders.rangeQuery(SessionToken.DYNAMIC_EXPIRES_AT).lt(entry.getValue())));
        }

        ActionListener<SearchResponse> searchListener = new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse response) {

                if (response.getHits().getHits().length == 0) {
                    if (log.isDebugEnabled()) {
                        log.debug("No updates for dynamic_expires needed");
                    }
                    
                    componentState.setState(State.INITIALIZED, "Flushed " + lastAccessCopy.size() + " dynamic_expires entries");

                    return;
                }

                if (log.isDebugEnabled()) {
                    log.debug("Got response for dynamic_expires search. Must update " + response.getHits().getHits().length + " tokens");
                }

                BulkRequest bulkRequest = new BulkRequest(indexName);

                for (SearchHit hit : response.getHits().getHits()) {
                    UpdateRequest updateRequest = new UpdateRequest(indexName, hit.getId());
                    updateRequest.setIfPrimaryTerm(hit.getPrimaryTerm());
                    updateRequest.setIfSeqNo(hit.getSeqNo());
                    updateRequest.doc(SessionToken.DYNAMIC_EXPIRES_AT, lastAccessCopy.get(hit.getId()));
                    bulkRequest.add(updateRequest);
                }

                privilegedConfigClient.bulk(bulkRequest, new ActionListener<BulkResponse>() {

                    @Override
                    public void onResponse(BulkResponse response) {
                        int updated = 0;
                        int conflict = 0;

                        List<String> failureMessages = new ArrayList<>();

                        for (BulkItemResponse itemResponse : response.getItems()) {
                            if (itemResponse.isFailed()) {
                                if (itemResponse.getFailure().getCause() instanceof VersionConflictEngineException) {
                                    // expected, ignore
                                    conflict++;
                                } else {
                                    failureMessages.add(Strings.toString(itemResponse.getFailure()));
                                    lastAccess.putIfAbsent(itemResponse.getId(), lastAccessCopy.get(itemResponse.getId()));
                                }
                            } else {
                                updated++;
                            }
                        }

                        if (failureMessages.isEmpty()) {
                            if (log.isDebugEnabled()) {
                                log.debug("Writing auth token activity bulk request finished. Updated: " + updated + "; Conflicts: " + conflict);
                            }
                        } else {
                            log.error("Error while writing auth token activity:\n" + failureMessages);
                            componentState.addLastException("session activity update", new Exception(failureMessages.toString()));
                            flushWithJitter(30);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        log.error("Error while writing session activity: " + query, e);
                        componentState.addLastException("session activity update", e);
                        restoreLastAccess(lastAccessCopy);
                    }
                });

                // Continue scrolling
                privilegedConfigClient.searchScroll(new SearchScrollRequest(response.getScrollId()), this);

            }

            @Override
            public void onFailure(Exception e) {
                log.error("Error while writing auth token activity: " + query, e);
                restoreLastAccess(lastAccessCopy);
            }

        };
        
        componentState.setState(State.INITIALIZED, "Flushing " + lastAccessCopy.size() + " dynamic_expires entries");
        
        privilegedConfigClient.search(new SearchRequest(indexName).source(new SearchSourceBuilder().query(query).size(1000)).scroll(new TimeValue(10000)), searchListener);

    }
    
    

    private void restoreLastAccess(Map<String, Long> lastAccessCopy) {
        for (Map.Entry<String, Long> entry : lastAccessCopy.entrySet()) {
            lastAccess.putIfAbsent(entry.getKey(), entry.getValue());
        }

        flushWithJitter(30);
    }

    public ComponentState getComponentState() {
        return componentState;
    }
}
