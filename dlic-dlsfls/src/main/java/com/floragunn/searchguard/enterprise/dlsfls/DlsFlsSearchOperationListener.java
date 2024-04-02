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

package com.floragunn.searchguard.enterprise.dlsfls;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.search.internal.SearchContext;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.floragunn.searchsupport.cstate.metrics.Meter;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import com.floragunn.searchsupport.cstate.metrics.TimeAggregation;

public class DlsFlsSearchOperationListener implements SearchOperationListener, ComponentStateProvider {
    private static final Logger log = LogManager.getLogger(DlsFlsSearchOperationListener.class);

    private final DlsFlsBaseContext dlsFlsBaseContext;
    private final AtomicReference<DlsFlsProcessedConfig> config;
    private final ComponentState componentState = new ComponentState(1, null, "search_operation_listener", DlsFlsSearchOperationListener.class)
            .initialized();
    private final TimeAggregation onPreQueryPhaseAggregation = new TimeAggregation.Nanoseconds();

    DlsFlsSearchOperationListener(DlsFlsBaseContext dlsFlsBase, AtomicReference<DlsFlsProcessedConfig> config) {
        this.dlsFlsBaseContext = dlsFlsBase;
        this.config = config;
        this.componentState.addMetrics("filter_pre_query_phase", onPreQueryPhaseAggregation);
    }

    @Override
    public void onPreQueryPhase(SearchContext searchContext) {
        DlsFlsProcessedConfig config = this.config.get();
        
        if (!config.isEnabled()) {
            log.trace("DlsFlsSearchOperationListener.onPreQueryPhase()\nnot enabled");
            return;
        }
        
        if (config.getDlsFlsConfig().getDlsMode() ==  DlsFlsConfig.Mode.FILTER_LEVEL) {
            log.trace("DlsFlsSearchOperationListener.onPreQueryPhase()\nFilter Level mode active");
            return;
        }

        if (dlsFlsBaseContext.isDlsDoneOnFilterLevel()) {
            log.trace("DlsFlsSearchOperationListener.onPreQueryPhase()\nisDlsDoneOnFilterLevel");
            return;
        }

        if (searchContext.suggest() != null) {
            log.trace("DlsFlsSearchOperationListener.onPreQueryPhase()\nisuggest: " + searchContext.suggest());
            return;
        }

        PrivilegesEvaluationContext privilegesEvaluationContext = dlsFlsBaseContext.getPrivilegesEvaluationContext();

        if (privilegesEvaluationContext == null) {
            log.trace("DlsFlsSearchOperationListener.onPreQueryPhase()\nno privilegesEvaluationContext");
            return;
        }

        try (Meter meter = Meter.detail(config.getMetricsLevel(), onPreQueryPhaseAggregation)) {

            RoleBasedDocumentAuthorization documentAuthorization = config.getDocumentAuthorization();

            if (documentAuthorization == null) {
                throw new IllegalStateException("Authorization configuration is not yet initialized");
            }

            String index = searchContext.indexShard().indexSettings().getIndex().getName();

            if (privilegesEvaluationContext.getSpecialPrivilegesEvaluationContext() != null
                    && privilegesEvaluationContext.getSpecialPrivilegesEvaluationContext().getRolesConfig() != null) {
                SgDynamicConfiguration<Role> roles = privilegesEvaluationContext.getSpecialPrivilegesEvaluationContext().getRolesConfig();
                documentAuthorization = new RoleBasedDocumentAuthorization(roles, ImmutableSet.of(index), MetricsLevel.NONE);
            }

            DlsRestriction dlsRestriction = documentAuthorization.getDlsRestriction(privilegesEvaluationContext, index, meter);
            
            log.trace("DlsRestriction for {}: {}", index, dlsRestriction);

            if (!dlsRestriction.isUnrestricted()) {
                if (config.getDlsFlsConfig().getDlsMode() ==  DlsFlsConfig.Mode.ADAPTIVE && dlsRestriction.containsTermLookupQuery()) {
                    // Special case for scroll operations: 
                    // Normally, the check dlsFlsBaseContext.isDlsDoneOnFilterLevel() already aborts early if DLS filter level mode
                    // has been activated. However, this is not the case for scroll operations, as these lose the thread context value
                    // on which dlsFlsBaseContext.isDlsDoneOnFilterLevel() is based on. Thus, we need to check here again the deeper
                    // conditions.
                    log.trace("DlsRestriction: contains TLQ.");
                    return;
                }
                                
                BooleanQuery.Builder queryBuilder = dlsRestriction.toBooleanQueryBuilder(searchContext.getSearchExecutionContext(),
                        (q) -> new ConstantScoreQuery(q));

                queryBuilder.add(searchContext.parsedQuery().query(), Occur.MUST);

                searchContext.parsedQuery(new ParsedQuery(queryBuilder.build()));
                searchContext.preProcess();
            }
        } catch (Exception e) {
            this.componentState.addLastException("filter_pre_query_phase", e);
            throw new RuntimeException("Error evaluating dls for a search query: " + e, e);
        }
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }
}
