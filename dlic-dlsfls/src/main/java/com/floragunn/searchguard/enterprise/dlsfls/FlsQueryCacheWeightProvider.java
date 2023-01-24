/*
  * Copyright 2022 by floragunn GmbH - All rights reserved
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

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.SearchGuardModule;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.floragunn.searchsupport.cstate.metrics.Meter;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import com.floragunn.searchsupport.cstate.metrics.TimeAggregation;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Weight;
import org.elasticsearch.index.Index;

public class FlsQueryCacheWeightProvider implements SearchGuardModule.QueryCacheWeightProvider, ComponentStateProvider {
    private static final Logger log = LogManager.getLogger(FlsQueryCacheWeightProvider.class);

    private final AtomicReference<DlsFlsProcessedConfig> config;
    private final DlsFlsBaseContext baseContext;
    private final ComponentState componentState = new ComponentState(12, null, "fls_query_cache_weight_provider", FlsQueryCacheWeightProvider.class)
            .initialized();
    private final TimeAggregation applyAggregation = new TimeAggregation.Nanoseconds();

    FlsQueryCacheWeightProvider(DlsFlsBaseContext baseContext, AtomicReference<DlsFlsProcessedConfig> config) {
        this.config = config;
        this.baseContext = baseContext;
        this.componentState.addMetrics("apply", applyAggregation);
    }

    @Override
    public Weight apply(Index index, Weight weight, QueryCachingPolicy policy) {
        DlsFlsProcessedConfig config = this.config.get();

        if (!config.isEnabled()) {
            return null;
        }

        PrivilegesEvaluationContext context = baseContext.getPrivilegesEvaluationContext();

        if (context == null) {
            return null;
        }

        try (Meter meter = Meter.detail(config.getMetricsLevel(), applyAggregation)) {
            RoleBasedFieldAuthorization fieldAuthorization = config.getFieldAuthorization();
            RoleBasedFieldMasking fieldMasking = config.getFieldMasking();

            if (context.getSpecialPrivilegesEvaluationContext() != null && context.getSpecialPrivilegesEvaluationContext().getRolesConfig() != null) {
                SgDynamicConfiguration<Role> roles = context.getSpecialPrivilegesEvaluationContext().getRolesConfig();
                ImmutableSet<String> indices = ImmutableSet.of(index.getName());
                fieldAuthorization = new RoleBasedFieldAuthorization(roles, indices, MetricsLevel.NONE);
                fieldMasking = new RoleBasedFieldMasking(roles, fieldMasking.getFieldMaskingConfig(), indices, MetricsLevel.NONE);
            }

            if (fieldAuthorization.hasFlsRestrictions(context, index.getName(), meter)
                    || fieldMasking.hasFieldMaskingRestrictions(context, index.getName(), meter)) {
                return weight;
            } else {
                return null;
            }
        } catch (Exception e) {
            log.error("Error in FlsQueryCacheWeightProvider.apply() for index " + index, e);
            componentState.addLastException("apply", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }
}
