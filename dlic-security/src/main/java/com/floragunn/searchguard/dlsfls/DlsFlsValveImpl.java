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
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.RealtimeRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.sampler.DiversifiedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantTermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.searchguard.GuiceDependencies;
import com.floragunn.searchguard.configuration.DlsFlsRequestValve;
import com.floragunn.searchguard.dlsfls.filter.DlsFilterLevelActionHandler;
import com.floragunn.searchguard.resolver.IndexResolverReplacer.Resolved;
import com.floragunn.searchguard.sgconf.EvaluatedDlsFlsConfig;
import com.floragunn.searchguard.support.Base64Helper;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.HeaderHelper;
import com.floragunn.searchguard.support.SgUtils;

public class DlsFlsValveImpl implements DlsFlsRequestValve {
    private static final String MAP_EXECUTION_HINT = "map";
    private static final Logger log = LogManager.getLogger(DlsFlsValveImpl.class);

    private final Client nodeClient;
    private final ClusterService clusterService;
    private final GuiceDependencies guiceDependencies;
    private final ThreadContext threadContext;
    private final Mode mode;
    private final DlsQueryParser dlsQueryParser;
    private final IndexNameExpressionResolver resolver;

    public DlsFlsValveImpl(Settings settings, Client nodeClient, ClusterService clusterService, IndexNameExpressionResolver resolver,
            GuiceDependencies guiceDependencies, NamedXContentRegistry namedXContentRegistry, ThreadContext threadContext) {
        super();
        this.nodeClient = nodeClient;
        this.clusterService = clusterService;
        this.resolver = resolver;
        this.guiceDependencies = guiceDependencies;
        this.threadContext = threadContext;
        this.mode = Mode.get(settings);
        this.dlsQueryParser = new DlsQueryParser(namedXContentRegistry);
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

        EvaluatedDlsFlsConfig filteredDlsFlsConfig = evaluatedDlsFlsConfig.filter(resolved);

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
                    log.debug("Doing lucene-level DLS because query does not contain TLQ");
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
                    guiceDependencies.getIndicesService(), resolver, dlsQueryParser, threadContext);
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
                    BooleanQuery.Builder queryBuilder = dlsQueryParser.parse(unparsedDlsQueries, context.getQueryShardContext(),
                            (q) -> new ConstantScoreQuery(q));

                    queryBuilder.add(context.parsedQuery().query(), Occur.MUST);

                    ParsedQuery dlsQuery = new ParsedQuery(queryBuilder.build());

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
}
