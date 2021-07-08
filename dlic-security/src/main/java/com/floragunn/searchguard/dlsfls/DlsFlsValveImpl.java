/*
 * Copyright 2016-2021 by floragunn GmbH - All rights reserved
 * 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 
 * This software is free of charge for non-commercial and academic use. 
 * For commercial use in a production environment you have to obtain a license 
 * from https://floragunn.com
 * 
 */

package com.floragunn.searchguard.dlsfls;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.RealtimeRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.sampler.DiversifiedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.InternalTerms;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantTermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;

import com.floragunn.searchguard.GuiceDependencies;
import com.floragunn.searchguard.configuration.DlsFlsRequestValve;
import com.floragunn.searchguard.dlsfls.filter.DlsFilterLevelActionHandler;
import com.floragunn.searchguard.dlsfls.filter.DlsFilterLevelRequestHandler;
import com.floragunn.searchguard.resolver.IndexResolverReplacer.Resolved;
import com.floragunn.searchguard.sgconf.EvaluatedDlsFlsConfig;
import com.floragunn.searchguard.support.Base64Helper;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.HeaderHelper;
import com.floragunn.searchguard.support.SgUtils;

public class DlsFlsValveImpl implements DlsFlsRequestValve {
    private static final String MAP_EXECUTION_HINT = "map";
    private static final Logger log = LogManager.getLogger(DlsFlsValveImpl.class);

    private static final Field REDUCE_ORDER_FIELD = getField(InternalTerms.class, "reduceOrder");
    private static final Field BUCKET_TERM_BYTES = getField(StringTerms.Bucket.class, "termBytes");
    private static final Field BUCKET_FORMAT = getField(InternalTerms.Bucket.class, "format");

    private final Client nodeClient;
    private final ClusterService clusterService;
    private final GuiceDependencies guiceDependencies;
    private final ThreadContext threadContext;
    private final Mode mode;
    private final DlsQueryParser dlsQueryParser;
    private final DlsFilterLevelRequestHandler dlsFilterLevelRequestHandler;

    public DlsFlsValveImpl(Settings settings, Client nodeClient, ClusterService clusterService, GuiceDependencies guiceDependencies,
            NamedXContentRegistry namedXContentRegistry, ThreadContext threadContext) {
        super();
        this.nodeClient = nodeClient;
        this.clusterService = clusterService;
        this.guiceDependencies = guiceDependencies;
        this.threadContext = threadContext;
        this.mode = Mode.get(settings);
        this.dlsQueryParser = new DlsQueryParser(namedXContentRegistry);
        this.dlsFilterLevelRequestHandler = new DlsFilterLevelRequestHandler(namedXContentRegistry, threadContext);
    }

    /**
     * 
     * @param request
     * @param listener
     * @return false on error
     */
    public boolean invoke(String action, ActionRequest request, final ActionListener<?> listener, EvaluatedDlsFlsConfig evaluatedDlsFlsConfig,
            final boolean localHashingEnabled, final Resolved resolved) {

        if (log.isDebugEnabled()) {
            log.debug("DlsFlsValveImpl.invoke()\nrequest: " + request + "\nevaluatedDlsFlsConfig: " + evaluatedDlsFlsConfig + "\nresolved: "
                    + resolved + "\nmode: " + mode);
        }

        if (evaluatedDlsFlsConfig == null || evaluatedDlsFlsConfig.isEmpty()) {
            return true;
        }

        if (threadContext.getHeader(ConfigConstants.SG_FILTER_LEVEL_DLS_DONE) != null) {
            if (log.isDebugEnabled()) {
                log.debug("DLS is already done for: " + threadContext.getHeader(ConfigConstants.SG_FILTER_LEVEL_DLS_DONE));
            }

            return true;
        }

        EvaluatedDlsFlsConfig filteredDlsFlsConfig = evaluatedDlsFlsConfig.filter(resolved.getAllIndices());

        boolean doFilterLevelDls;

        if (mode == Mode.FILTER_LEVEL) {
            doFilterLevelDls = true;
        } else if (mode == Mode.LUCENE_LEVEL) {
            doFilterLevelDls = false;
        } else { // mode == Mode.ADAPTIVE
            Mode modeByHeader = getDlsModeHeader();

            if (modeByHeader == Mode.FILTER_LEVEL) {
                doFilterLevelDls = true;
                log.debug("Doing filter-level DLS due to header");
            } else {
                doFilterLevelDls = dlsQueryParser.containsTermLookupQuery(filteredDlsFlsConfig.getAllQueries());

                if (doFilterLevelDls) {
                    setDlsModeHeader(Mode.FILTER_LEVEL);
                    log.debug("Doing filter-level DLS because query contains TLQ");
                } else {
                    log.debug("Doing lucene-level DLS because query does not contain TLQ", new Exception());
                }
            }
        }

        if (!doFilterLevelDls) {
            setDlsHeaders(evaluatedDlsFlsConfig, request);
        }

        setFlsHeaders(evaluatedDlsFlsConfig, request);

        if (filteredDlsFlsConfig.isEmpty()) {
            return true;
        }

        if (request instanceof RealtimeRequest) {
            ((RealtimeRequest) request).realtime(Boolean.FALSE);
        }

        if (request instanceof SearchRequest) {

            SearchRequest searchRequest = ((SearchRequest) request);

            //When we encounter a terms or sampler aggregation with masked fields activated we forcibly
            //need to switch off global ordinals because field masking can break ordering
            //https://www.elastic.co/guide/en/elasticsearch/reference/master/eager-global-ordinals.html#_avoiding_global_ordinal_loading
            if (evaluatedDlsFlsConfig.hasFieldMasking()) {

                if (searchRequest.source() != null && searchRequest.source().aggregations() != null) {
                    for (AggregationBuilder aggregationBuilder : searchRequest.source().aggregations().getAggregatorFactories()) {
                        if (aggregationBuilder instanceof TermsAggregationBuilder) {
                            ((TermsAggregationBuilder) aggregationBuilder).executionHint(MAP_EXECUTION_HINT);
                        }

                        if (aggregationBuilder instanceof SignificantTermsAggregationBuilder) {
                            ((SignificantTermsAggregationBuilder) aggregationBuilder).executionHint(MAP_EXECUTION_HINT);
                        }

                        if (aggregationBuilder instanceof DiversifiedAggregationBuilder) {
                            ((DiversifiedAggregationBuilder) aggregationBuilder).executionHint(MAP_EXECUTION_HINT);
                        }
                    }
                }
            }

            if (localHashingEnabled && !evaluatedDlsFlsConfig.hasFls() && !evaluatedDlsFlsConfig.hasDls()
                    && searchRequest.source().aggregations() != null) {

                boolean cacheable = true;

                for (AggregationBuilder af : searchRequest.source().aggregations().getAggregatorFactories()) {

                    if (!af.getType().equals("cardinality") && !af.getType().equals("count")) {
                        cacheable = false;
                        continue;
                    }

                    StringBuilder sb = new StringBuilder();

                    if (searchRequest.source() != null) {
                        sb.append(Strings.toString(searchRequest.source()) + System.lineSeparator());
                    }

                    sb.append(Strings.toString(af) + System.lineSeparator());

                    LogManager.getLogger("debuglogger").error(sb.toString());

                }

                if (!cacheable) {
                    searchRequest.requestCache(Boolean.FALSE);
                } else {
                    LogManager.getLogger("debuglogger").error("Shard requestcache enabled for "
                            + (searchRequest.source() == null ? "<NULL>" : Strings.toString(searchRequest.source())));
                }

            } else {
                searchRequest.requestCache(Boolean.FALSE);
            }
        }

        if (request instanceof UpdateRequest) {
            listener.onFailure(new ElasticsearchSecurityException("Update is not supported when FLS or DLS or Fieldmasking is activated"));
            return false;
        }

        if (request instanceof BulkRequest) {
            for (DocWriteRequest<?> inner : ((BulkRequest) request).requests()) {
                if (inner instanceof UpdateRequest) {
                    listener.onFailure(new ElasticsearchSecurityException("Update is not supported when FLS or DLS or Fieldmasking is activated"));
                    return false;
                }
            }
        }

        if (request instanceof BulkShardRequest) {
            for (BulkItemRequest inner : ((BulkShardRequest) request).items()) {
                if (inner.request() instanceof UpdateRequest) {
                    listener.onFailure(new ElasticsearchSecurityException("Update is not supported when FLS or DLS or Fieldmasking is activated"));
                    return false;
                }
            }
        }

        if (request instanceof ResizeRequest) {
            listener.onFailure(new ElasticsearchSecurityException("Resize is not supported when FLS or DLS or Fieldmasking is activated"));
            return false;
        }

        if (evaluatedDlsFlsConfig.hasDls()) {
            if (request instanceof SearchRequest) {

                final SearchSourceBuilder source = ((SearchRequest) request).source();
                if (source != null) {

                    if (source.profile()) {
                        listener.onFailure(new ElasticsearchSecurityException("Profiling is not supported when DLS is activated"));
                        return false;
                    }

                }
            }
        }

        if (doFilterLevelDls && filteredDlsFlsConfig.hasDls()) {
            return DlsFilterLevelActionHandler.handle(action, request, listener, evaluatedDlsFlsConfig, resolved, nodeClient, clusterService,
                    guiceDependencies.getIndicesService(), dlsQueryParser, threadContext);
        } else {
            return true;
        }
    }

    @Override
    public void handleSearchContext(SearchContext context, ThreadPool threadPool, NamedXContentRegistry namedXContentRegistry) {
        try {
            @SuppressWarnings("unchecked")
            final Map<String, Set<String>> queries = (Map<String, Set<String>>) HeaderHelper.deserializeSafeFromHeader(threadPool.getThreadContext(),
                    ConfigConstants.SG_DLS_QUERY_HEADER);

            final String dlsEval = SgUtils.evalMap(queries, context.indexShard().indexSettings().getIndex().getName());

            if (dlsEval != null) {

                if (context.suggest() != null) {
                    return;
                }

                assert context.parsedQuery() != null;

                final Set<String> unparsedDlsQueries = queries.get(dlsEval);
                if (unparsedDlsQueries != null && !unparsedDlsQueries.isEmpty()) {
                    final ParsedQuery dlsQuery = dlsQueryParser.parseForValve(unparsedDlsQueries, context.parsedQuery(),
                            context.getSearchExecutionContext());

                    if (dlsQuery != null) {
                        context.parsedQuery(dlsQuery);
                        context.preProcess(true);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error evaluating dls for a search query: " + e, e);
        }

    }

    @Override
    public void onQueryPhase(SearchContext searchContext, long tookInNanos, ThreadPool threadPool) {
        QuerySearchResult queryResult = searchContext.queryResult();
        if (queryResult == null) {
            return;
        }

        DelayableWriteable<InternalAggregations> aggregationsDelayedWritable = queryResult.aggregations();
        if (aggregationsDelayedWritable == null) {
            return;
        }

        if (!isFieldMaskingConfigured(threadPool)) {
            return;
        }

        InternalAggregations aggregations = aggregationsDelayedWritable.expand();
        if (aggregations == null) {
            return;
        }

        if (checkForCorrectReduceOrder(aggregations)) {
            return;
        }

        //If this assert is not raised during integration tests we should consider to remove the entire onQueryPhase() method from this class
        assert false : "Because of the fact we now use no longer global ordinals for terms aggregations when masked fields are enabled this correction feature shoudl no longer be necessary.";

        if (log.isDebugEnabled()) {
            log.debug("Found buckets with equal keys. Merging these buckets: " + aggregations);
        }

        ArrayList<InternalAggregation> modifiedAggregations = new ArrayList<>(aggregations.asList().size() + 1);

        for (Aggregation aggregation : aggregations) {
            if (!(aggregation instanceof StringTerms)) {
                modifiedAggregations.add((InternalAggregation) aggregation);
                continue;
            }

            StringTerms stringTerms = (StringTerms) aggregation;
            BucketOrder reduceOrder = getReduceOrder(stringTerms);

            if (checkForCorrectReduceOrder(reduceOrder, stringTerms.getBuckets())) {
                modifiedAggregations.add((StringTerms) aggregation);
                continue;
            }

            List<StringTerms.Bucket> buckets = sortAndMergeBucketKeys(reduceOrder, stringTerms.getBuckets());

            StringTerms modifiedStringTerms = stringTerms.create(buckets);

            modifiedAggregations.add(modifiedStringTerms);
        }

        queryResult.aggregations(InternalAggregations.from(modifiedAggregations));

    }

    private boolean isFieldMaskingConfigured(ThreadPool threadPool) {
        @SuppressWarnings("unchecked")
        Map<String, Set<String>> maskedFieldsMap = (Map<String, Set<String>>) HeaderHelper.deserializeSafeFromHeader(threadPool.getThreadContext(),
                ConfigConstants.SG_MASKED_FIELD_HEADER);

        return (maskedFieldsMap != null && !maskedFieldsMap.isEmpty());
    }

    private boolean checkForCorrectReduceOrder(InternalAggregations aggregations) {
        for (Aggregation aggregation : aggregations) {
            if (!(aggregation instanceof StringTerms)) {
                continue;
            }

            StringTerms stringTerms = (StringTerms) aggregation;
            BucketOrder reduceOrder = getReduceOrder(stringTerms);

            if (!checkForCorrectReduceOrder(reduceOrder, stringTerms.getBuckets())) {

                if (log.isDebugEnabled()) {
                    log.debug("Aggregation needs correction: " + stringTerms + " " + reduceOrder);
                }

                return false;
            }

        }

        return true;
    }

    private boolean checkForCorrectReduceOrder(BucketOrder reduceOrder, List<StringTerms.Bucket> buckets) {
        Comparator<Bucket> comparator = reduceOrder.comparator();
        StringTerms.Bucket prevBucket = null;

        for (StringTerms.Bucket bucket : buckets) {
            if (prevBucket == null) {
                prevBucket = bucket;
                continue;
            }

            if (comparator.compare(prevBucket, bucket) >= 0) {
                return false;
            }

            prevBucket = bucket;
        }

        return true;
    }

    private List<StringTerms.Bucket> sortAndMergeBucketKeys(BucketOrder reduceOrder, List<StringTerms.Bucket> buckets) {
        Comparator<Bucket> comparator = reduceOrder.comparator();
        int bucketCount = buckets.size();
        StringTerms.Bucket[] bucketArray = buckets.toArray(new StringTerms.Bucket[bucketCount]);
        ArrayList<StringTerms.Bucket> result = new ArrayList<StringTerms.Bucket>(bucketCount);

        Arrays.sort(bucketArray, comparator);

        if (log.isDebugEnabled()) {
            log.debug("Merging buckets: " + buckets.stream().map(b -> b.getKeyAsString()).collect(Collectors.toList()));
        }

        for (int i = 0; i < bucketCount;) {
            StringTerms.Bucket currentBucket = bucketArray[i];

            if (i + 1 < bucketCount && comparator.compare(currentBucket, bucketArray[i + 1]) == 0) {
                int k = i + 1;
                long mergedDocCount = currentBucket.getDocCount();
                long mergedDocCountError;

                try {
                    mergedDocCountError = currentBucket.getDocCountError();
                } catch (IllegalStateException e) {
                    mergedDocCountError = -1;
                }

                do {
                    StringTerms.Bucket equalKeyBucket = bucketArray[k];
                    mergedDocCount += equalKeyBucket.getDocCount();

                    if (mergedDocCountError != -1) {
                        try {
                            mergedDocCountError += equalKeyBucket.getDocCountError();
                        } catch (IllegalStateException e) {
                            mergedDocCountError = -1;
                        }
                    }

                    k++;
                } while (k < bucketCount && comparator.compare(currentBucket, bucketArray[k]) == 0);

                result.add(new StringTerms.Bucket(getTerm(currentBucket), mergedDocCount, (InternalAggregations) currentBucket.getAggregations(),
                        mergedDocCountError != -1, mergedDocCountError, getDocValueFormat(currentBucket)));

                i = k;

            } else {
                result.add(currentBucket);
                i++;
            }

        }

        if (log.isDebugEnabled()) {
            log.debug("New buckets: " + result.stream().map(b -> b.getKeyAsString()).collect(Collectors.toList()));
        }

        return result;
    }

    private static BucketOrder getReduceOrder(InternalTerms<?, ?> aggregation) {
        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        return AccessController.doPrivileged((PrivilegedAction<BucketOrder>) () -> {
            try {
                return (BucketOrder) REDUCE_ORDER_FIELD.get(aggregation);
            } catch (IllegalArgumentException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static BytesRef getTerm(StringTerms.Bucket bucket) {
        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        return AccessController.doPrivileged((PrivilegedAction<BytesRef>) () -> {
            try {
                return (BytesRef) BUCKET_TERM_BYTES.get(bucket);
            } catch (IllegalArgumentException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static DocValueFormat getDocValueFormat(InternalTerms.Bucket<?> bucket) {
        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        return AccessController.doPrivileged((PrivilegedAction<DocValueFormat>) () -> {
            try {
                return (DocValueFormat) BUCKET_FORMAT.get(bucket);
            } catch (IllegalArgumentException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static Field getField(Class<?> clazz, String name) {
        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        return AccessController.doPrivileged((PrivilegedAction<Field>) () -> {

            try {
                Field field = clazz.getDeclaredField(name);
                field.setAccessible(true);
                return field;
            } catch (NoSuchFieldException | SecurityException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void setDlsHeaders(EvaluatedDlsFlsConfig dlsFls, ActionRequest request) {
        if (!dlsFls.getDlsQueriesByIndex().isEmpty()) {
            Map<String, Set<String>> dlsQueries = dlsFls.getDlsQueriesByIndex();

            if (request instanceof ClusterSearchShardsRequest && HeaderHelper.isTrustedClusterRequest(threadContext)) {
                threadContext.addResponseHeader(ConfigConstants.SG_DLS_QUERY_HEADER, Base64Helper.serializeObject((Serializable) dlsQueries));
                if (log.isDebugEnabled()) {
                    log.debug("added response header for DLS info: {}", dlsQueries);
                }
            } else {
                if (threadContext.getHeader(ConfigConstants.SG_DLS_QUERY_HEADER) != null) {
                    Object deserializedDlsQueries = Base64Helper.deserializeObject(threadContext.getHeader(ConfigConstants.SG_DLS_QUERY_HEADER));
                    if (!dlsQueries.equals(deserializedDlsQueries)) {                        
                        throw new ElasticsearchSecurityException(ConfigConstants.SG_DLS_QUERY_HEADER + " does not match (SG 900D)");
                    }
                } else {
                    threadContext.putHeader(ConfigConstants.SG_DLS_QUERY_HEADER, Base64Helper.serializeObject((Serializable) dlsQueries));
                    if (log.isDebugEnabled()) {
                        log.debug("attach DLS info: {}", dlsQueries);
                    }
                }
            }
        }
    }

    private void setDlsModeHeader(Mode mode) {
        String modeString = mode.name();

        if (threadContext.getHeader(ConfigConstants.SG_DLS_MODE_HEADER) != null) {
            if (!modeString.equals(threadContext.getHeader(ConfigConstants.SG_DLS_MODE_HEADER))) {
                log.warn("Cannot update DLS mode to " + mode + "; current: " + threadContext.getHeader(ConfigConstants.SG_DLS_MODE_HEADER));
            }
        } else {
            threadContext.putHeader(ConfigConstants.SG_DLS_MODE_HEADER, modeString);
        }
    }

    private Mode getDlsModeHeader() {
        String modeString = threadContext.getHeader(ConfigConstants.SG_DLS_MODE_HEADER);

        if (modeString != null) {
            return Mode.valueOf(modeString);
        } else {
            return null;
        }
    }

    private void setFlsHeaders(EvaluatedDlsFlsConfig dlsFls, ActionRequest request) {
        if (!dlsFls.getFieldMaskingByIndex().isEmpty()) {
            Map<String, Set<String>> maskedFieldsMap = dlsFls.getFieldMaskingByIndex();

            if (request instanceof ClusterSearchShardsRequest && HeaderHelper.isTrustedClusterRequest(threadContext)) {
                threadContext.addResponseHeader(ConfigConstants.SG_MASKED_FIELD_HEADER, Base64Helper.serializeObject((Serializable) maskedFieldsMap));
                if (log.isDebugEnabled()) {
                    log.debug("added response header for masked fields info: {}", maskedFieldsMap);
                }
            } else {

                if (threadContext.getHeader(ConfigConstants.SG_MASKED_FIELD_HEADER) != null) {
                    if (!maskedFieldsMap.equals(Base64Helper.deserializeObject(threadContext.getHeader(ConfigConstants.SG_MASKED_FIELD_HEADER)))) {
                        throw new ElasticsearchSecurityException(ConfigConstants.SG_MASKED_FIELD_HEADER + " does not match (SG 901D)");
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug(ConfigConstants.SG_MASKED_FIELD_HEADER + " already set");
                        }
                    }
                } else {
                    threadContext.putHeader(ConfigConstants.SG_MASKED_FIELD_HEADER, Base64Helper.serializeObject((Serializable) maskedFieldsMap));
                    if (log.isDebugEnabled()) {
                        log.debug("attach masked fields info: {}", maskedFieldsMap);
                    }
                }
            }
        }

        if (!dlsFls.getFlsByIndex().isEmpty()) {
            Map<String, Set<String>> flsFields = dlsFls.getFlsByIndex();

            if (request instanceof ClusterSearchShardsRequest && HeaderHelper.isTrustedClusterRequest(threadContext)) {
                threadContext.addResponseHeader(ConfigConstants.SG_FLS_FIELDS_HEADER, Base64Helper.serializeObject((Serializable) flsFields));
                if (log.isDebugEnabled()) {
                    log.debug("added response header for FLS info: {}", flsFields);
                }
            } else {
                if (threadContext.getHeader(ConfigConstants.SG_FLS_FIELDS_HEADER) != null) {
                    if (!flsFields.equals(Base64Helper.deserializeObject(threadContext.getHeader(ConfigConstants.SG_FLS_FIELDS_HEADER)))) {
                        throw new ElasticsearchSecurityException(ConfigConstants.SG_FLS_FIELDS_HEADER + " does not match (SG 901D) " + flsFields
                                + "---" + Base64Helper.deserializeObject(threadContext.getHeader(ConfigConstants.SG_FLS_FIELDS_HEADER)));
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug(ConfigConstants.SG_FLS_FIELDS_HEADER + " already set");
                        }
                    }
                } else {
                    threadContext.putHeader(ConfigConstants.SG_FLS_FIELDS_HEADER, Base64Helper.serializeObject((Serializable) flsFields));
                    if (log.isDebugEnabled()) {
                        log.debug("attach FLS info: {}", flsFields);
                    }
                }
            }

        }
    }

    public static enum Mode {
        ADAPTIVE, LUCENE_LEVEL, FILTER_LEVEL;

        static Mode get(Settings settings) {
            String modeString = settings.get(ConfigConstants.SEARCHGUARD_DLS_MODE);

            if ("adaptive".equalsIgnoreCase(modeString)) {
                return Mode.ADAPTIVE;
            } else if ("lucene_level".equalsIgnoreCase(modeString)) {
                return Mode.LUCENE_LEVEL;
            } else if ("filter_level".equalsIgnoreCase(modeString)) {
                return Mode.FILTER_LEVEL;
            } else {
                return Mode.ADAPTIVE;
            }
        }
    }

    @Override
    public <T extends TransportRequest> T handleRequest(T request, TransportChannel channel, Task task) {
        return dlsFilterLevelRequestHandler.handle(request, channel, task);
    };
}
