/*
 * Copyright 2022 floragunn GmbH
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Weight;
import org.elasticsearch.index.Index;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.SearchGuardModule;
import com.floragunn.searchguard.authc.AuthInfoService;
import com.floragunn.searchguard.authz.AuthorizationService;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContext;
import com.floragunn.searchguard.user.User;

public class FlsQueryCacheWeightProvider implements SearchGuardModule.QueryCacheWeightProvider {
    private static final Logger log = LogManager.getLogger(FlsQueryCacheWeightProvider.class);

    private final AtomicReference<DlsFlsProcessedConfig> config;
    private final AuthInfoService authInfoService;
    private final AuthorizationService authorizationService;

    FlsQueryCacheWeightProvider(AtomicReference<DlsFlsProcessedConfig> config, AuthInfoService authInfoService,
            AuthorizationService authorizationService) {
        this.config = config;
        this.authInfoService = authInfoService;
        this.authorizationService = authorizationService;
    }

    @Override
    public Weight apply(Index index, Weight weight, QueryCachingPolicy policy) {
        try {
            DlsFlsProcessedConfig config = this.config.get();

            if (!config.isEnabled()) {
                return null;
            }
            
            PrivilegesEvaluationContext context = getPrivilegesEvaluationContext();

            if (context == null) {
                return null;
            }

            RoleBasedFieldAuthorization fieldAuthorization = config.getFieldAuthorization();
            RoleBasedFieldMasking fieldMasking = config.getFieldMasking();

            if (context.getSpecialPrivilegesEvaluationContext() != null && context.getSpecialPrivilegesEvaluationContext().getRolesConfig() != null) {
                SgDynamicConfiguration<Role> roles = context.getSpecialPrivilegesEvaluationContext().getRolesConfig();
                ImmutableSet<String> indices = ImmutableSet.of(index.getName());
                fieldAuthorization = new RoleBasedFieldAuthorization(roles, indices);
                fieldMasking = new RoleBasedFieldMasking(roles, fieldMasking.getFieldMaskingConfig(), indices);
            }
            
            if (fieldAuthorization.hasFlsRestrictions(context, index.getName())
                    || fieldMasking.hasFieldMaskingRestrictions(context, index.getName())) {
                return weight;
            } else {
                return null;
            }
        } catch (Exception e) {
            log.error("Error in FlsQueryCacheWeightProvider.apply() for index " + index, e);
            throw new RuntimeException(e);
        }
    }

    private PrivilegesEvaluationContext getPrivilegesEvaluationContext() {
        User user = authInfoService.peekCurrentUser();
        
        if (user == null) {
            return null;
        }
        
        SpecialPrivilegesEvaluationContext specialPrivilegesEvaluationContext = authInfoService.getSpecialPrivilegesEvaluationContext();
        ImmutableSet<String> mappedRoles = this.authorizationService.getMappedRoles(user, specialPrivilegesEvaluationContext);

        return new PrivilegesEvaluationContext(user, mappedRoles, null, null, false, null, specialPrivilegesEvaluationContext, null);
    }
}
