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

package com.floragunn.searchguard.enterprise.dlsfls;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.RealtimeRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.sampler.DiversifiedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantTermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.GuiceDependencies;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.SyncAuthorizationFilter;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.enterprise.dlsfls.DlsFlsConfig.Mode;
import com.floragunn.searchguard.enterprise.dlsfls.filter.DlsFilterLevelActionHandler;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.floragunn.searchsupport.cstate.metrics.Meter;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import com.floragunn.searchsupport.cstate.metrics.TimeAggregation;

import static org.elasticsearch.rest.RestStatus.INTERNAL_SERVER_ERROR;

public class DlsFlsValve implements SyncAuthorizationFilter, ComponentStateProvider {
    private static final String MAP_EXECUTION_HINT = "map";
    private static final String DIRECT_EXECUTION_HINT = "direct";
    private static final Logger log = LogManager.getLogger(DlsFlsValve.class);

    private final Client nodeClient;
    private final ClusterService clusterService;
    private final GuiceDependencies guiceDependencies;
    private final ThreadContext threadContext;
    private final IndexNameExpressionResolver resolver;
    private final AtomicReference<DlsFlsProcessedConfig> config;
    private final ComponentState componentState = new ComponentState(0, null, "dls_fls_valve", DlsFlsValve.class).initialized();
    private final TimeAggregation applyTimeAggregation = new TimeAggregation.Nanoseconds();

    public DlsFlsValve(Client nodeClient, ClusterService clusterService, IndexNameExpressionResolver resolver, GuiceDependencies guiceDependencies,
            ThreadContext threadContext, AtomicReference<DlsFlsProcessedConfig> config) {
        this.nodeClient = nodeClient;
        this.clusterService = clusterService;
        this.resolver = resolver;
        this.guiceDependencies = guiceDependencies;
        this.threadContext = threadContext;
        this.config = config;
        this.componentState.addMetrics("filter_request", applyTimeAggregation);
    }

    @Override
    public SyncAuthorizationFilter.Result apply(PrivilegesEvaluationContext context, ActionListener<?> listener) {
        if (threadContext.getHeader(ConfigConstants.SG_FILTER_LEVEL_DLS_DONE) != null) {
            if (log.isDebugEnabled()) {
                log.debug("DLS is already done for: " + threadContext.getHeader(ConfigConstants.SG_FILTER_LEVEL_DLS_DONE));
            }

            return SyncAuthorizationFilter.Result.OK;
        }

        DlsFlsProcessedConfig config = this.config.get();

        try (Meter meter = Meter.detail(config.getMetricsLevel(), applyTimeAggregation)) {
            RoleBasedDocumentAuthorization documentAuthorization = config.getDocumentAuthorization();
            RoleBasedFieldAuthorization fieldAuthorization = config.getFieldAuthorization();
            RoleBasedFieldMasking fieldMasking = config.getFieldMasking();
            DlsFlsConfig.Mode mode = config.getDlsFlsConfig().getDlsMode();

            if (!config.isEnabled()) {
                return SyncAuthorizationFilter.Result.OK;
            }

            blockAccessInCaseOfRoleOrMappingsConfigurationErrors();

            if (log.isDebugEnabled()) {
                log.debug("DlsFlsValveImpl.invoke()\nrequest: " + context.getRequest() + "\nresolved: "
                        + context.getRequestInfo().getResolvedIndices() + "\nmode: " + mode);
            }

            ImmutableSet<String> indices = context.getRequestInfo().getResolvedIndices().getLocalIndices();

            if (context.getSpecialPrivilegesEvaluationContext() != null && context.getSpecialPrivilegesEvaluationContext().getRolesConfig() != null) {
                SgDynamicConfiguration<Role> roles = context.getSpecialPrivilegesEvaluationContext().getRolesConfig();
                documentAuthorization = new RoleBasedDocumentAuthorization(roles, indices, MetricsLevel.NONE);
                fieldAuthorization = new RoleBasedFieldAuthorization(roles, indices, MetricsLevel.NONE);
                fieldMasking = new RoleBasedFieldMasking(roles, fieldMasking.getFieldMaskingConfig(), indices, MetricsLevel.NONE);
            }

            boolean hasDlsRestrictions = documentAuthorization.hasDlsRestrictions(context, indices, meter);
            boolean hasFlsRestrictions = fieldAuthorization.hasFlsRestrictions(context, indices, meter);
            boolean hasFieldMasking = fieldMasking.hasFieldMaskingRestrictions(context, indices, meter);

            if (!hasDlsRestrictions && !hasFlsRestrictions && !hasFieldMasking) {
                return SyncAuthorizationFilter.Result.OK;
            }

            DlsRestriction.IndexMap restrictionMap = null;

            boolean doFilterLevelDls;

            if (mode == Mode.FILTER_LEVEL) {
                doFilterLevelDls = true;
                restrictionMap = documentAuthorization.getDlsRestriction(context, indices, meter);
            } else if (mode == Mode.LUCENE_LEVEL) {
                doFilterLevelDls = false;
            } else { // mode == Mode.ADAPTIVE
                Mode modeByHeader = getDlsModeHeader();

                if (modeByHeader == Mode.FILTER_LEVEL) {
                    doFilterLevelDls = true;
                    log.debug("Doing filter-level DLS due to header");
                } else {
                    restrictionMap = documentAuthorization.getDlsRestriction(context, indices, meter);
                    doFilterLevelDls = restrictionMap.containsTermLookupQuery();

                    if (doFilterLevelDls) {
                        setDlsModeHeader(Mode.FILTER_LEVEL);
                        log.debug("Doing filter-level DLS because query contains TLQ");
                    } else {
                        log.debug("Doing lucene-level DLS because query does not contain TLQ");
                    }
                }
            }

            Object request = context.getRequest();

            if (request instanceof RealtimeRequest) {
                ((RealtimeRequest) request).realtime(Boolean.FALSE);
            }

            if (request instanceof SearchRequest) {
                SearchRequest searchRequest = ((SearchRequest) request);
                if (searchRequest.source() != null && searchRequest.source().aggregations() != null) {
                    for (AggregationBuilder factory : searchRequest.source().aggregations().getAggregatorFactories()) {
                        if (factory instanceof TermsAggregationBuilder && ((TermsAggregationBuilder) factory).minDocCount() == 0) {
                            if (config.getDlsFlsConfig().isForceMinDocCountToOne()) {
                                log.debug("Forcing terms aggregation min doc count to 1");
                                ((TermsAggregationBuilder) factory).minDocCount(1);
                            } else {
                                return SyncAuthorizationFilter.Result.DENIED.reason("min_doc_count 0 is not supported when DLS is activated");
                            }
                        }
                    }
                }

                if (hasFieldMasking) {
                    if (searchRequest.source() != null && searchRequest.source().aggregations() != null) {
                        //When we encounter a terms or sampler aggregation with masked fields activated we forcibly
                        //need to switch off global ordinals because field masking can break ordering
                        //https://www.elastic.co/guide/en/elasticsearch/reference/master/eager-global-ordinals.html#_avoiding_global_ordinal_loading
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

                            //force direct execution mode in case of cardinality aggregation
                            if (aggregationBuilder instanceof CardinalityAggregationBuilder) {
                                ((CardinalityAggregationBuilder) aggregationBuilder).executionHint(DIRECT_EXECUTION_HINT);
                            }
                        }
                    }
                }

                searchRequest.requestCache(Boolean.FALSE);
            }

            if (hasDlsRestrictions) {
                if (request instanceof SearchRequest) {

                    final SearchSourceBuilder source = ((SearchRequest) request).source();
                    if (source != null) {

                        if (source.profile()) {
                            return SyncAuthorizationFilter.Result.DENIED.reason("Profiling is not supported when DLS is activated");
                        }
                    }
                }
            }

            if (doFilterLevelDls) {
                return DlsFilterLevelActionHandler.handle(context.getAction(), (ActionRequest) request, listener, restrictionMap,
                        context.getRequestInfo().getResolvedIndices(), nodeClient, clusterService, guiceDependencies.getIndicesService(), resolver,
                        threadContext);
            } else {
                return SyncAuthorizationFilter.Result.OK;
            }
        } catch (PrivilegesEvaluationException e) {
            log.error("Error while evaluating DLS/FLS privileges", e);
            componentState.addLastException("filter_request", e);
            return SyncAuthorizationFilter.Result.DENIED.reason(e.getMessage()).cause(e);
        } catch (RuntimeException e) {
            log.error("Error while evaluating DLS/FLS privileges", e);
            componentState.addLastException("filter_request_u", e);
            throw e;
        }
    }

    private void blockAccessInCaseOfRoleOrMappingsConfigurationErrors() {
        DlsFlsProcessedConfig dlsFlsProcessedConfig = config.get();
        if ((dlsFlsProcessedConfig != null) && dlsFlsProcessedConfig.containsValidationError()) {
            log.error(dlsFlsProcessedConfig.getValidationErrorDescription());
            throw new ElasticsearchStatusException(//
                "Incorrect configuration of SearchGuard roles or roles mapping, please check the log file for more details. ("//
                    + dlsFlsProcessedConfig.getUniqueValidationErrorToken() + ")", INTERNAL_SERVER_ERROR);
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
            return DlsFlsConfig.Mode.valueOf(modeString);
        } else {
            return null;
        }
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

}
