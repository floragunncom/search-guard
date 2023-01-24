/*
  * Copyright 2015-2022 by floragunn GmbH - All rights reserved
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
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.enterprise.dlsfls.RoleBasedFieldAuthorization.FlsRule;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.floragunn.searchsupport.cstate.metrics.Meter;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import com.floragunn.searchsupport.cstate.metrics.TimeAggregation;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FlsFieldFilter implements Function<String, Predicate<String>>, ComponentStateProvider {
    private static final String KEYWORD = ".keyword";
    private static final Logger log = LogManager.getLogger(FlsFieldFilter.class);

    private final DlsFlsBaseContext baseContext;

    private final AtomicReference<DlsFlsProcessedConfig> config;
    private final ComponentState componentState = new ComponentState(1, null, "fls_field_filter", FlsFieldFilter.class).initialized();
    private final TimeAggregation applyAggregation = new TimeAggregation.Nanoseconds();

    FlsFieldFilter(DlsFlsBaseContext baseContext, AtomicReference<DlsFlsProcessedConfig> config) {
        this.baseContext = baseContext;
        this.config = config;
        this.componentState.addMetrics("filter_fields", applyAggregation);
    }

    @Override
    public Predicate<String> apply(String index) {
        DlsFlsProcessedConfig config = this.config.get();

        if (!config.isEnabled()) {
            return (field) -> true;
        }

        PrivilegesEvaluationContext privilegesEvaluationContext = this.baseContext.getPrivilegesEvaluationContext();

        if (privilegesEvaluationContext == null) {
            return (field) -> true;
        }

        try (Meter meter = Meter.detail(config.getMetricsLevel(), applyAggregation)) {
            RoleBasedFieldAuthorization fieldAuthorization = config.getFieldAuthorization();

            if (fieldAuthorization == null) {
                throw new IllegalStateException("FLS is not initialized");
            }

            if (privilegesEvaluationContext.getSpecialPrivilegesEvaluationContext() != null
                    && privilegesEvaluationContext.getSpecialPrivilegesEvaluationContext().getRolesConfig() != null) {
                SgDynamicConfiguration<Role> roles = privilegesEvaluationContext.getSpecialPrivilegesEvaluationContext().getRolesConfig();
                fieldAuthorization = new RoleBasedFieldAuthorization(roles, ImmutableSet.of(index), MetricsLevel.NONE);
            }

            FlsRule flsRule = fieldAuthorization.getFlsRule(privilegesEvaluationContext, index, meter);
            return (field) -> flsRule.isAllowed(removeSuffix(field));
        } catch (PrivilegesEvaluationException e) {
            log.error("Error while evaluating FLS for index " + index, e);
            componentState.addLastException("filter_fields", e);
            throw new RuntimeException("Error while evaluating FLS for index " + index, e);
        } catch (RuntimeException e) {
            log.error("Error while evaluating FLS for index " + index, e);
            componentState.addLastException("filter_fields", e);
            throw e;
        }
    }

    private static String removeSuffix(String field) {
        if (field != null && field.endsWith(KEYWORD)) {
            return field.substring(0, field.length() - KEYWORD.length());
        }
        return field;
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

}
