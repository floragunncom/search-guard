/*
 * Copyright 2016-2022 by floragunn GmbH - All rights reserved
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
/*
 * Includes parts from https://github.com/opensearch-project/security/blob/c18a50ac4c5f7116e0e7c3411944d1438f9c44e9/src/main/java/org/opensearch/security/configuration/DlsFlsValveImpl.java
 * 
 * Copyright OpenSearch Contributors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.floragunn.searchguard.enterprise.dlsfls.legacy;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.RealtimeRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchShardsRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.sampler.DiversifiedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantTermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.GuiceDependencies;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.SyncAuthorizationFilter;
import com.floragunn.searchguard.authz.actions.ResolvedIndices;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.enterprise.dlsfls.legacy.filter.DlsFilterLevelActionHandler;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContext;
import com.floragunn.searchguard.support.Base64Helper;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.HeaderHelper;
import com.floragunn.searchguard.user.User;

import static org.elasticsearch.rest.RestStatus.INTERNAL_SERVER_ERROR;

public class DlsFlsValve implements SyncAuthorizationFilter {
    private static final String MAP_EXECUTION_HINT = "map";
    private static final String DIRECT_EXECUTION_HINT = "direct";
    private static final Logger log = LogManager.getLogger(DlsFlsValve.class);

    private final Client nodeClient;
    private final ClusterService clusterService;
    private final GuiceDependencies guiceDependencies;
    private final ThreadContext threadContext;
    private final Mode mode;
    private final DlsQueryParser dlsQueryParser;
    private final IndexNameExpressionResolver resolver;
    private final AtomicReference<DlsFlsProcessedConfig> config;
    private final boolean localHashingEnabled;

    public DlsFlsValve(Settings settings, Client nodeClient, ClusterService clusterService, IndexNameExpressionResolver resolver,
            GuiceDependencies guiceDependencies, NamedXContentRegistry namedXContentRegistry, ThreadContext threadContext,
            ConfigurationRepository configurationRepository, AtomicReference<DlsFlsProcessedConfig> config, DlsFlsComplianceConfig complianceConfig) {
        this.nodeClient = nodeClient;
        this.clusterService = clusterService;
        this.resolver = resolver;
        this.guiceDependencies = guiceDependencies;
        this.threadContext = threadContext;
        this.mode = Mode.get(settings);
        this.dlsQueryParser = new DlsQueryParser(namedXContentRegistry);
        this.config = config;
        this.localHashingEnabled = complianceConfig.isLocalHashingEnabled();
    }

    @Override
    public SyncAuthorizationFilter.Result apply(PrivilegesEvaluationContext context, ActionListener<?> listener) {

        DlsFlsProcessedConfig config = this.config.get();

        if (!config.isEnabled()) {
            return SyncAuthorizationFilter.Result.OK;
        }

        blockAccessInCaseOfRoleOrMappingsConfigurationErrors();

        User user = context.getUser();
        ImmutableSet<String> mappedRoles = context.getMappedRoles();
        String action = context.getAction().name();
        ActionRequest request = (ActionRequest) context.getRequest();
        ResolvedIndices resolved = context.getRequestInfo().getResolvedIndices();
        SpecialPrivilegesEvaluationContext specialPrivilegesEvaluationContext = context.getSpecialPrivilegesEvaluationContext();

        if (threadContext.getHeader(ConfigConstants.SG_FILTER_LEVEL_DLS_DONE) != null) {
            if (log.isDebugEnabled()) {
                log.debug("DLS is already done for: " + threadContext.getHeader(ConfigConstants.SG_FILTER_LEVEL_DLS_DONE));
            }

            return SyncAuthorizationFilter.Result.OK;
        }

        if (log.isDebugEnabled()) {
            log.debug("DlsFlsValveImpl.invoke()\nrequest: " + request + "\nresolved: " + resolved + "\nmode: " + mode);
        }

        LegacyRoleBasedDocumentAuthorization documentAuthorization = config.getDocumentAuthorization();
        
        if (specialPrivilegesEvaluationContext != null && specialPrivilegesEvaluationContext.getRolesConfig() != null) {
            SgDynamicConfiguration<Role> roles = context.getSpecialPrivilegesEvaluationContext().getRolesConfig();
            documentAuthorization = new LegacyRoleBasedDocumentAuthorization(roles, resolver, clusterService);
        }

        EvaluatedDlsFlsConfig evaluatedDlsFlsConfig;
        try {
            evaluatedDlsFlsConfig = documentAuthorization.getDlsFlsConfig(user, mappedRoles, null);
        } catch (PrivilegesEvaluationException e) {
            log.error("Error while evaluating DLS/FLS configuration", e);
            throw new ElasticsearchSecurityException("Error while evaluating DLS/FLS configuration", e);
        }

        if (evaluatedDlsFlsConfig == null || evaluatedDlsFlsConfig.isEmpty()) {
            return SyncAuthorizationFilter.Result.OK;
        }

        // We need to use the unfiltered DLS configuration if the request does not contain any index information. This
        // is the case for example for scroll requests.
        EvaluatedDlsFlsConfig filteredDlsFlsConfig = resolved != null ? evaluatedDlsFlsConfig.filter(resolved) : evaluatedDlsFlsConfig;

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
            return SyncAuthorizationFilter.Result.OK;
        }

        if (request instanceof RealtimeRequest) {
            ((RealtimeRequest) request).realtime(Boolean.FALSE);
        }

        if (request instanceof SearchRequest) {

            SearchRequest searchRequest = ((SearchRequest) request);
            if (searchRequest.source() != null && searchRequest.source().aggregations() != null) {
                for (AggregationBuilder factory : searchRequest.source().aggregations().getAggregatorFactories()) {
                    if (factory instanceof TermsAggregationBuilder && ((TermsAggregationBuilder) factory).minDocCount() == 0) {
                        return SyncAuthorizationFilter.Result.DENIED.reason("min_doc_count 0 is not supported when DLS is activated");
                    }
                }
            }

            if (evaluatedDlsFlsConfig.hasFieldMasking()) {

                if (searchRequest.source() != null && searchRequest.source().aggregations() != null) {
                    for (AggregationBuilder aggregationBuilder : searchRequest.source().aggregations().getAggregatorFactories()) {
                        //When we encounter a terms or sampler aggregation with masked fields activated we forcibly
                        //need to switch off global ordinals because field masking can break ordering
                        //https://www.elastic.co/guide/en/elasticsearch/reference/master/eager-global-ordinals.html#_avoiding_global_ordinal_loading

                        if (aggregationBuilder instanceof TermsAggregationBuilder) {
                            ((TermsAggregationBuilder) aggregationBuilder).executionHint(MAP_EXECUTION_HINT);
                        }

                        if (aggregationBuilder instanceof SignificantTermsAggregationBuilder) {
                            ((SignificantTermsAggregationBuilder) aggregationBuilder).executionHint(MAP_EXECUTION_HINT);
                        }

                        if (aggregationBuilder instanceof DiversifiedAggregationBuilder) {
                            ((DiversifiedAggregationBuilder) aggregationBuilder).executionHint(MAP_EXECUTION_HINT);
                        }

                        //force direct execution mode in case of cardinality aggregation
                        if (aggregationBuilder instanceof CardinalityAggregationBuilder) {
                            ((CardinalityAggregationBuilder) aggregationBuilder).executionHint(DIRECT_EXECUTION_HINT);
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
                        break;
                    }
                }

                if (!cacheable) {
                    searchRequest.requestCache(Boolean.FALSE);
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
                        return SyncAuthorizationFilter.Result.DENIED.reason("Profiling is not supported when DLS is activated");
                    }

                }
            }
        }

        if (doFilterLevelDls && filteredDlsFlsConfig.hasDls()) {
            return DlsFilterLevelActionHandler.handle(action, request, listener, evaluatedDlsFlsConfig, resolved, nodeClient, clusterService,
                    guiceDependencies.getIndicesService(), resolver, dlsQueryParser, threadContext);
        } else {
            return SyncAuthorizationFilter.Result.OK;
        }
    }

    private void blockAccessInCaseOfRoleOrMappingsConfigurationErrors() {
        DlsFlsProcessedConfig dlsFlsProcessedConfig = config.get();
        if ((dlsFlsProcessedConfig != null) && dlsFlsProcessedConfig.containsValidationError()) {
            log.error(dlsFlsProcessedConfig.getValidationErrorDescription());
            String msg = "Incorrect configuration of SearchGuard roles or roles mapping, please check the log file for more details. ("//
                + dlsFlsProcessedConfig.getUniqueValidationErrorToken() + ")";
            throw new ElasticsearchStatusException(msg, INTERNAL_SERVER_ERROR);
        }
    }

    private void setDlsHeaders(EvaluatedDlsFlsConfig dlsFls, ActionRequest request) {
        if (!dlsFls.getDlsQueriesByIndex().isEmpty()) {
            Map<String, Set<String>> dlsQueries = dlsFls.getDlsQueriesByIndex();

            if ((request instanceof ClusterSearchShardsRequest || request instanceof SearchShardsRequest) && HeaderHelper.isTrustedClusterRequest(threadContext)) {
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

            if ((request instanceof ClusterSearchShardsRequest || request instanceof SearchShardsRequest) && HeaderHelper.isTrustedClusterRequest(threadContext)) {
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

            if ((request instanceof ClusterSearchShardsRequest || request instanceof SearchShardsRequest) && HeaderHelper.isTrustedClusterRequest(threadContext)) {
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

    public DlsQueryParser getDlsQueryParser() {
        return dlsQueryParser;
    }
}
