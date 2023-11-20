/*
 * Copyright 2015-2024 floragunn GmbH
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

import java.util.Collection;

import com.floragunn.codova.config.templates.ExpressionEvaluationException;
import com.floragunn.codova.config.templates.Template;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchsupport.cstate.metrics.Meter;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import com.floragunn.searchsupport.meta.Meta;
import com.floragunn.searchsupport.queries.Query;

public class RoleBasedDocumentAuthorization extends RoleBasedAuthorizationBase<RoleBasedDocumentAuthorization.DlsQuery, DlsRestriction> {

    public RoleBasedDocumentAuthorization(SgDynamicConfiguration<Role> roles, Meta indexMetadata, MetricsLevel metricsLevel) {
        super(roles, indexMetadata, metricsLevel, RoleBasedDocumentAuthorization::roleToRule);
    }

    static DlsQuery roleToRule(Role.Index rolePermissions) {
        Template<Query> dlsQueryTemplate = rolePermissions.getDls();

        if (dlsQueryTemplate != null) {
            return new DlsQuery(dlsQueryTemplate);
        } else {
            return null;
        }
    }

    @Override
    protected DlsRestriction unrestricted() {
        return DlsRestriction.NONE;
    }


    @Override
    protected DlsRestriction fullyRestricted() {
        return DlsRestriction.FULL;
    }

    @Override
    protected DlsRestriction compile(PrivilegesEvaluationContext context, Collection<DlsQuery> rules) throws PrivilegesEvaluationException {
        ImmutableList.Builder<com.floragunn.searchsupport.queries.Query> renderedQueries = new ImmutableList.Builder<>(rules.size());

        for (DlsQuery query : rules) {
            try {
                renderedQueries.add(query.queryTemplate.render(context.getUser()));
            } catch (ExpressionEvaluationException e) {
                throw new PrivilegesEvaluationException("Error while rendering query " + query, e);
            }
        }

        return new DlsRestriction(renderedQueries.build());
    }

    @Override
    protected String hasRestrictionsMetricName() {
        return "has_dls_restrictions";
    }

    @Override
    protected String evaluateRestrictionsMetricName() {
        return "evaluate_dls_restrictions";
    }

    @Override
    protected String componentName() {
        return "role_based_document_authorization";
    }

    public DlsRestriction.IndexMap getRestriction(PrivilegesEvaluationContext context, Collection<Meta.Index> indices, Meter meter)
            throws PrivilegesEvaluationException {
        try (Meter subMeter = meter.detail("evaluate_dls")) {
            if (this.staticIndexRules.rolesWithIndexWildcardWithoutRule.containsAny(context.getMappedRoles())) {
                return DlsRestriction.IndexMap.NONE;
            }

            ImmutableMap.Builder<Meta.Index, DlsRestriction> result = new ImmutableMap.Builder<>(indices.size());

            int restrictedIndices = 0;

            for (Meta.Index index : indices) {
                DlsRestriction restriction = getRestrictionImpl(context, index);

                if (!restriction.isUnrestricted()) {
                    restrictedIndices++;
                }

                result.put(index, restriction);
            }

            if (restrictedIndices == 0) {
                return DlsRestriction.IndexMap.NONE;
            }

            return new DlsRestriction.IndexMap(result.build());
        } catch (PrivilegesEvaluationException e) {
            componentState.addLastException("get_dls_restriction", e);
            throw e;
        } catch (RuntimeException e) {
            componentState.addLastException("get_dls_restriction_u", e);
            throw e;
        }
    }

    static class DlsQuery {
        final Template<com.floragunn.searchsupport.queries.Query> queryTemplate;

        DlsQuery(Template<com.floragunn.searchsupport.queries.Query> queryTemplate) {
            this.queryTemplate = queryTemplate;
        }

        @Override
        public int hashCode() {
            return queryTemplate.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof DlsQuery)) {
                return false;
            }
            DlsQuery other = (DlsQuery) obj;
            if (queryTemplate == null) {
                if (other.queryTemplate != null) {
                    return false;
                }
            } else if (!queryTemplate.equals(other.queryTemplate)) {
                return false;
            }
            return true;
        }
    }

}
