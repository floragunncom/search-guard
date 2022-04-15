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
import java.time.ZoneOffset;
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
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.searchguard.modules.state.ComponentState;
import com.floragunn.searchguard.modules.state.ComponentState.State;
import com.floragunn.searchguard.support.PrivilegedConfigClient;

/**
 * This class is responsible for tracking the last access to a session and extending the expiration time of that session on that access.
 * 
 * As a session may be accessed on any node of the cluster, the nodes need to coordinate between each other the information about the
 * current expiration times of the active sessions.
 * 
 * In order to avoid an index write on each access of a session, this class keeps the most recently accessed sessions in heap 
 * (in the ConcurrentHashMap lastAccess). This is regularly batch-wise synced to the session index.
 *
 */
class SessionActivityTracker {
    private static final Logger log = LogManager.getLogger(SessionActivityTracker.class);

    private final String indexName;
    private final SessionService sessionService;
    private final PrivilegedConfigClient privilegedConfigClient;

    /**
     * Maps session ids to the expiry time of the session (measured in milliseconds since epoch).
     * 
     * This map keeps ONLY sessions where the expiry time deviates from the expiry time stored in the index. When this map is synced to the index, this map is emptied.
     * Afterwards, the information from the index is the only valid information.
     */
    private final Map<String, Long> lastAccess = new ConcurrentHashMap<>();
    private final ThreadPool threadPool;
    private Duration minFlushInterval = Duration.ofSeconds(10);
    private Duration inactivityTimeout;
    private long inactivityBeforeExpiryMillis;
    private Duration flushInterval;
    private Random random;
    private final ComponentState componentState = new ComponentState(1, null, "session_activity_tracker", SessionActivityTracker.class).initialized();
    private volatile long lastComponentStateUpdate = -1;
    private WriteRequest.RefreshPolicy indexRefreshPolicy = WriteRequest.RefreshPolicy.NONE;

    SessionActivityTracker(Duration inactivityTimeout, SessionService sessionService, String indexName, PrivilegedConfigClient privilegedConfigClient,
            ThreadPool threadPool) {
        this.indexName = indexName;
        this.sessionService = sessionService;
        this.privilegedConfigClient = privilegedConfigClient;
        this.threadPool = threadPool;
        this.random = new Random(System.currentTimeMillis() + hashCode());

        setInactivityTimeout(inactivityTimeout);

        this.sessionFlushThread = new SessionFlushThread();
        this.sessionFlushThread.start();
    }

    void trackAccess(SessionToken authToken) {
        long now = System.currentTimeMillis();

        lastAccess.put(authToken.getId(), now + inactivityBeforeExpiryMillis);
        Instant nextFlushBeforeUpdate = sessionFlushThread.nextFlush;
        sessionFlushThread.setEarlyFlushTimeIfNecessary(authToken.getDynamicExpiryTime().toEpochMilli());

        log.info("Next flush: " + nextFlushBeforeUpdate + " " + sessionFlushThread.nextFlush + " << " + authToken.getDynamicExpiryTime());

        if (lastComponentStateUpdate + 10 * 1000 < now) {
            lastComponentStateUpdate = now;
        }

    }

    void checkExpiryAndTrackAccess(SessionToken authToken, Consumer<Boolean> onResult, Consumer<Exception> onFailure) {
        Instant now = Instant.now();

        if (authToken.getDynamicExpiryTime().isAfter(now)) {
            // all good
            if (log.isTraceEnabled()) {
                log.trace("Checked timeout of " + authToken.getId() + "; token is not timed out. Expiry: " + authToken.getDynamicExpiryTime() + " ("
                        + authToken.getDynamicExpiryTime().atZone(ZoneOffset.systemDefault()) + ")");
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
                        + authToken.getDynamicExpiryTime() + " (" + authToken.getDynamicExpiryTime().atZone(ZoneOffset.systemDefault()) + ")");
            }

            trackAccess(authToken);
            onResult.accept(Boolean.TRUE);
            return;
        }

        //  Now do a double-check by reading the token from the index.

        if (log.isTraceEnabled()) {
            log.trace("The token " + authToken.getId() + " seems to have expired; re-checking with fresh index data. Expiry: "
                    + authToken.getDynamicExpiryTime() + " (" + authToken.getDynamicExpiryTime().atZone(ZoneOffset.systemDefault()) + ")");
        }

        sessionService.getByIdFromIndex(authToken.getId(), (refreshedAuthToken) -> {
            if (refreshedAuthToken.getDynamicExpiryTime().isAfter(now)) {
                // all good

                if (log.isTraceEnabled()) {
                    log.trace("Checked timeout of " + authToken.getId() + "; token is not timed out (needed index recheck). Expiry: "
                            + refreshedAuthToken.getDynamicExpiryTime() + " ("
                            + refreshedAuthToken.getDynamicExpiryTime().atZone(ZoneOffset.systemDefault()) + ")");
                }

                trackAccess(authToken);
                onResult.accept(Boolean.TRUE);
            } else {
                if (log.isInfoEnabled()) {
                    log.info("The auth token " + authToken.getId() + " is expired. Expiry: " + refreshedAuthToken.getDynamicExpiryTime() + " ("
                            + refreshedAuthToken.getDynamicExpiryTime().atZone(ZoneOffset.systemDefault()) + ")");
                }

                onResult.accept(Boolean.FALSE);
            }

        }, (noSuchAuthTokenException) -> onResult.accept(Boolean.FALSE), onFailure);

    }

    Duration getInactivityTimeout() {
        return inactivityTimeout;
    }

    synchronized void setInactivityTimeout(Duration inactivityBeforeExpiry) {
        if (this.inactivityTimeout != null && inactivityBeforeExpiry.equals(this.inactivityTimeout)) {
            return;
        }

        this.inactivityTimeout = inactivityBeforeExpiry;
        this.inactivityBeforeExpiryMillis = inactivityBeforeExpiry.toMillis();

        Duration newFlushInterval = inactivityBeforeExpiry.minusSeconds(120);

        if (newFlushInterval.compareTo(minFlushInterval) < 0) {
            newFlushInterval = minFlushInterval;
        }

        this.flushInterval = newFlushInterval;

        if (this.sessionFlushThread != null) {
            this.sessionFlushThread.setEarlyFlushTime(Instant.now().plus(newFlushInterval).minusMillis(random.nextInt(2000)));
        }
    }

    void setIndexRefreshPolicy(WriteRequest.RefreshPolicy indexRefreshPolicy) {
        this.indexRefreshPolicy = indexRefreshPolicy;
    }

    private void flushWithJitter(String reason, int fixedDelay) {
        if (!lastAccess.isEmpty()) {
            if (this.flushInterval.toMillis() < 2 * 60 * 1000) {
                threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueMillis(random.nextInt(1000) + fixedDelay * 1000), ThreadPool.Names.GENERIC,
                        () -> this.flush(reason));
            } else {
                threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueSeconds(random.nextInt(20) + fixedDelay), ThreadPool.Names.GENERIC,
                        () -> this.flush(reason));
            }

        } else {
            if (log.isTraceEnabled()) {
                log.trace("Not running flush because lastAccess is empty");
            }
        }
    }

    private void flush(String reason) {
        try {
            Map<String, Long> lastAccessCopy = new HashMap<>(lastAccess);

            if (log.isDebugEnabled()) {
                log.debug("Flushing " + lastAccessCopy.size() + " dynamic_expires entries; reason: " + reason + "\n" + lastAccessCopy.keySet());
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
                            log.debug(
                                    "No updates for dynamic_expires needed; Flushed in total " + lastAccessCopy.size() + " dynamic_expires entries");
                        }

                        componentState.setState(State.INITIALIZED, "Flushed " + lastAccessCopy.size() + " dynamic_expires entries");

                        return;
                    }

                    if (log.isDebugEnabled()) {
                        log.debug("Got response for dynamic_expires search. Must update " + response.getHits().getHits().length + " tokens");
                    }

                    BulkRequest bulkRequest = new BulkRequest(indexName);
                    bulkRequest.setRefreshPolicy(indexRefreshPolicy);
                    List<String> sessionIdsToBeUpdated = new ArrayList<>(response.getHits().getHits().length);

                    for (SearchHit hit : response.getHits().getHits()) {
                        Long localDynamicExpiresAt = lastAccessCopy.get(hit.getId());

                        if (localDynamicExpiresAt != null) {
                            UpdateRequest updateRequest = new UpdateRequest(indexName, hit.getId());
                            sessionIdsToBeUpdated.add(hit.getId());
                            updateRequest.setIfPrimaryTerm(hit.getPrimaryTerm());
                            updateRequest.setIfSeqNo(hit.getSeqNo());
                            updateRequest.doc(SessionToken.DYNAMIC_EXPIRES_AT, localDynamicExpiresAt);
                            bulkRequest.add(updateRequest);
                        }
                    }

                    if (log.isTraceEnabled()) {
                        log.trace("Tokens for update " + sessionIdsToBeUpdated);
                    }

                    if (bulkRequest.numberOfActions() != 0) {
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
                                        log.debug("Writing auth token activity bulk request finished. Updated: " + updated + "; Conflicts: "
                                                + conflict);
                                    }
                                } else {
                                    log.error("Error while writing auth token activity:\n" + failureMessages);
                                    componentState.addLastException("session activity update", new Exception(failureMessages.toString()));
                                    flushWithJitter("retry after error", 30);
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                log.error("Error while writing session activity: " + query, e);
                                componentState.addLastException("session activity update", e);
                                restoreLastAccess(lastAccessCopy);
                            }
                        });
                    }

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

            privilegedConfigClient.search(
                    new SearchRequest(indexName).source(new SearchSourceBuilder().query(query).size(1000)).scroll(new TimeValue(10000)),
                    searchListener);
        } catch (Exception e) {
            log.error("Exception during flush(" + reason + ")", e);
        }
    }

    private void restoreLastAccess(Map<String, Long> lastAccessCopy) {
        for (Map.Entry<String, Long> entry : lastAccessCopy.entrySet()) {
            lastAccess.putIfAbsent(entry.getKey(), entry.getValue());
        }

        flushWithJitter("restoreLastAccess", 30);
    }

    public ComponentState getComponentState() {
        return componentState;
    }

    private final SessionFlushThread sessionFlushThread;

    private class SessionFlushThread extends Thread {

        private Instant nextFlush;

        SessionFlushThread() {
            super("sg_session_activity_flusher");
            setDaemon(true);
        }

        @Override
        public void run() {
            for (;;) {
                try {
                    synchronized (this) {
                        if (nextFlush == null) {
                            nextFlush = Instant.now().plus(flushInterval).minusMillis(random.nextInt(2000));
                        }

                        long millisToWait = nextFlush.toEpochMilli() - System.currentTimeMillis();

                        if (millisToWait > 0) {
                            wait(millisToWait + 10);

                            if (System.currentTimeMillis() < nextFlush.toEpochMilli()) {
                                // We got woken up early, possibly because the nextFlush was modified
                                continue;
                            }
                        }

                        nextFlush = Instant.now().plus(flushInterval);
                    }

                    if (log.isDebugEnabled()) {
                        log.debug("Flushing sessions now; next scheduled flush is at " + nextFlush);
                    }

                    flush("schedule");

                } catch (Exception e) {
                    log.error("Error in sg_session_activity_flusher", e);
                }
            }
        }

        synchronized void setEarlyFlushTime(Instant newNextFlush) {
            if (this.nextFlush == null || this.nextFlush.isAfter(newNextFlush)) {
                this.nextFlush = newNextFlush;
                notifyAll();
            }
        }

        synchronized void setEarlyFlushTimeIfNecessary(long individualSessionTimeout) {
            if (this.nextFlush == null || this.nextFlush.toEpochMilli() - 10000 > individualSessionTimeout) {
                this.nextFlush = Instant.ofEpochMilli(individualSessionTimeout - 10000);

                if (log.isTraceEnabled()) {
                    log.trace("Earlier flushing necessary. Now flushing at " + this.nextFlush);
                }

                notifyAll();
            }
        }

    }

}
