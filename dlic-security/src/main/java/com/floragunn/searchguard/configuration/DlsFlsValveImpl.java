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

package com.floragunn.searchguard.configuration;

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
import org.elasticsearch.indices.IndicesService;
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
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.searchguard.resolver.IndexResolverReplacer.Resolved;
import com.floragunn.searchguard.sgconf.EvaluatedDlsFlsConfig;
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
    private final NamedXContentRegistry namedXContentRegistry;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final ThreadContext threadContext;

    public DlsFlsValveImpl(Settings settings, Client nodeClient, ClusterService clusterService, IndicesService indicesService,
            NamedXContentRegistry namedXContentRegistry, ThreadContext threadContext) {
        super();
        this.nodeClient = nodeClient;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.namedXContentRegistry = namedXContentRegistry;
        this.threadContext = threadContext;
    }

    /**
     * 
     * @param request
     * @param listener
     * @return false on error
     */
    public boolean invoke(final ActionRequest request, final ActionListener<?> listener, final EvaluatedDlsFlsConfig evaluatedDlsFlsConfig,
            final boolean localHashingEnabled, final Resolved resolved) {

        if (log.isDebugEnabled()) {
            log.debug("DlsFlsValveImpl.invoke()\nrequest: " + request + "\nevaluatedDlsFlsConfig: " + evaluatedDlsFlsConfig + "\nresolved: "
                    + resolved);
        }

        if (evaluatedDlsFlsConfig == null || evaluatedDlsFlsConfig.isEmpty()) {
            return true;
        }

        if (request instanceof RealtimeRequest) {
            ((RealtimeRequest) request).realtime(Boolean.FALSE);
        }

        if (request instanceof SearchRequest) {

            SearchRequest sr = ((SearchRequest) request);

            if (localHashingEnabled && !evaluatedDlsFlsConfig.hasFls() && !evaluatedDlsFlsConfig.hasDls() && sr.source().aggregations() != null) {

                boolean cacheable = true;

                for (AggregationBuilder af : sr.source().aggregations().getAggregatorFactories()) {

                    if (!af.getType().equals("cardinality") && !af.getType().equals("count")) {
                        cacheable = false;
                        continue;
                    }

                    StringBuilder sb = new StringBuilder();

                    if (sr.source() != null) {
                        sb.append(Strings.toString(sr.source()) + System.lineSeparator());
                    }

                    sb.append(Strings.toString(af) + System.lineSeparator());

                    LogManager.getLogger("debuglogger").error(sb.toString());

                }

                if (!cacheable) {
                    sr.requestCache(Boolean.FALSE);
                } else {
                    LogManager.getLogger("debuglogger")
                            .error("Shard requestcache enabled for " + (sr.source() == null ? "<NULL>" : Strings.toString(sr.source())));
                }

            } else {
                sr.requestCache(Boolean.FALSE);
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

        if (evaluatedDlsFlsConfig.hasFilterLevelDlsQueries()) {
            return new DlsFilterLevelHandler(request, listener, evaluatedDlsFlsConfig, resolved, nodeClient, clusterService, indicesService,
                    namedXContentRegistry, threadContext).handle();
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
                    final ParsedQuery dlsQuery = DlsQueryParser.parseForValve(unparsedDlsQueries, context.parsedQuery(), context.getSearchExecutionContext(),
                            namedXContentRegistry, threadPool.getThreadContext());
                    
                    if(dlsQuery != null) {
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
        assert false:"Because of the fact we now use no longer global ordinals for terms aggregations when masked fields are enabled this correction feature shoudl no longer be necessary.";

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

}
