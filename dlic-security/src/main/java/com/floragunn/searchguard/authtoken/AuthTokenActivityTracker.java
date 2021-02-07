package com.floragunn.searchguard.authtoken;

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

import com.floragunn.searchguard.support.PrivilegedConfigClient;

public class AuthTokenActivityTracker {
    private static final Logger log = LogManager.getLogger(AuthTokenActivityTracker.class);

    private final String indexName;
    private final AuthTokenService authTokenService;
    private final PrivilegedConfigClient privilegedConfigClient;
    private final Map<String, Long> lastAccess = new ConcurrentHashMap<>();
    private final ThreadPool threadPool;
    private Duration minFlushInterval = Duration.ofSeconds(30);
    private Duration inactivityTimeout;
    private long inactivityBeforeExpiryMillis;
    private TimeValue flushInterval;
    private Cancellable job;
    private Random random;

    public AuthTokenActivityTracker(Duration inactivityTimeout, AuthTokenService authTokenService, String indexName,
            PrivilegedConfigClient privilegedConfigClient, ThreadPool threadPool) {
        this.indexName = indexName;
        this.authTokenService = authTokenService;
        this.privilegedConfigClient = privilegedConfigClient;
        this.threadPool = threadPool;
        this.random = new Random(System.currentTimeMillis() + hashCode());

        setInactivityTimeout(inactivityTimeout);
    }

    public void trackAccess(AuthToken authToken) {
        lastAccess.put(authToken.getId(), System.currentTimeMillis() + inactivityBeforeExpiryMillis);
    }

    public void checkExpiryAndTrackAccess(AuthToken authToken, Consumer<Boolean> onResult, Consumer<Exception> onFailure) {
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

        authTokenService.getByIdFromIndex(authToken.getId(), (refreshedAuthToken) -> {
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
                    .must(QueryBuilders.rangeQuery(AuthToken.DYNAMIC_EXPIRES_AT).lt(entry.getValue())));
        }

        // TODO information gap between start and finish
        // TODO scroll

        privilegedConfigClient.search(new SearchRequest(indexName).source(new SearchSourceBuilder().query(query).size(1000)),
                new ActionListener<SearchResponse>() {

                    @Override
                    public void onResponse(SearchResponse response) {

                        if (response.getHits().getHits().length == 0) {
                            if (log.isDebugEnabled()) {
                                log.debug("No updates for dynamic_expires needed");
                            }

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
                            updateRequest.doc(AuthToken.DYNAMIC_EXPIRES_AT, lastAccessCopy.get(hit.getId()));
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
                                        log.debug("Writing auth token activity bulk request finished. Updated: " + updated + "; Conflicts: "
                                                + conflict);
                                    }
                                } else {
                                    log.error("Error while writing auth token activity:\n" + failureMessages);
                                    flushWithJitter(30);
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                log.error("Error while writing auth token activity: " + query, e);
                                restoreLastAccess(lastAccessCopy);
                            }
                        });

                    }

                    @Override
                    public void onFailure(Exception e) {
                        log.error("Error while writing auth token activity: " + query, e);
                        restoreLastAccess(lastAccessCopy);
                    }

                });

    }

    private void restoreLastAccess(Map<String, Long> lastAccessCopy) {
        for (Map.Entry<String, Long> entry : lastAccessCopy.entrySet()) {
            lastAccess.putIfAbsent(entry.getKey(), entry.getValue());
        }

        flushWithJitter(30);
    }
}
