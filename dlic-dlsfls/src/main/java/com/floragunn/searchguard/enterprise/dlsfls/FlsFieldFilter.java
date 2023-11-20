/*
 * Copyright 2015-2022 floragunn GmbH
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

package com.floragunn.searchguard.enterprise.dlsfls;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.enterprise.dlsfls.RoleBasedFieldAuthorization.FlsRule;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.floragunn.searchsupport.cstate.metrics.Meter;
import com.floragunn.searchsupport.cstate.metrics.TimeAggregation;
import org.elasticsearch.plugins.FieldPredicate;
import com.floragunn.searchsupport.meta.Meta;

public class FlsFieldFilter implements Function<String, FieldPredicate>, ComponentStateProvider {
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
    public FieldPredicate apply(String indexName) {
        DlsFlsProcessedConfig config = this.config.get();

        if (!config.isEnabled()) {
            return FieldPredicate.ACCEPT_ALL;
        }

        PrivilegesEvaluationContext privilegesEvaluationContext = this.baseContext.getPrivilegesEvaluationContext();

        if (privilegesEvaluationContext == null) {
            return FieldPredicate.ACCEPT_ALL;
        }

        try (Meter meter = Meter.detail(config.getMetricsLevel(), applyAggregation)) {
            Meta.Index index = (Meta.Index) this.baseContext.getIndexMetaData().getIndexOrLike(indexName);
            RoleBasedFieldAuthorization fieldAuthorization = config.getFieldAuthorization();

            if (fieldAuthorization == null) {
                throw new IllegalStateException("FLS is not initialized");
            }

            if (privilegesEvaluationContext.getSpecialPrivilegesEvaluationContext() != null
                    && privilegesEvaluationContext.getSpecialPrivilegesEvaluationContext().getRolesConfig() != null) {
                SgDynamicConfiguration<Role> roles = privilegesEvaluationContext.getSpecialPrivilegesEvaluationContext().getRolesConfig();
                 fieldAuthorization = new RoleBasedFieldAuthorization(roles, baseContext.getIndexMetaData(), MetricsLevel.NONE);
            }

            FlsRule flsRule = fieldAuthorization.getRestriction(privilegesEvaluationContext, index, meter);
            return new FieldPredicate() {
                @Override
                public long ramBytesUsed() {
                    return 0; //todo
                }

                @Override
                public boolean test(String field) {
                    return flsRule.isAllowed(removeSuffix(field));
                }

                @Override
                public String modifyHash(String hash) {
                    return hash; //todo
                }
            };
        } catch (PrivilegesEvaluationException e) {
            log.error("Error while evaluating FLS for index " + indexName, e);
            componentState.addLastException("filter_fields", e);
            throw new RuntimeException("Error while evaluating FLS for index " + indexName, e);
        } catch (RuntimeException e) {
            log.error("Error while evaluating FLS for index " + indexName, e);
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
