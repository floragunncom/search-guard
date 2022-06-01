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
import java.util.function.Predicate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authc.AuthInfoService;
import com.floragunn.searchguard.authz.AuthorizationService;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.enterprise.dlsfls.RoleBasedFieldAuthorization.FlsRule;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContext;
import com.floragunn.searchguard.user.User;

public class FlsFieldFilter implements Function<String, Predicate<String>> {
    private static final String KEYWORD = ".keyword";
    private static final Logger log = LogManager.getLogger(FlsFieldFilter.class);

    private final AuthInfoService authInfoService;
    private final AuthorizationService authorizationService;

    private final AtomicReference<DlsFlsProcessedConfig> config;

    FlsFieldFilter(AuthInfoService authInfoService, AuthorizationService authorizationService, AtomicReference<DlsFlsProcessedConfig> config) {
        this.authInfoService = authInfoService;
        this.authorizationService = authorizationService;
        this.config = config;
    }

    @Override
    public Predicate<String> apply(String index) {
        DlsFlsProcessedConfig config = this.config.get();

        if (!config.isEnabled()) {
            return (field) -> true;
        }

        PrivilegesEvaluationContext privilegesEvaluationContext = getPrivilegesEvaluationContext();

        if (privilegesEvaluationContext == null) {
            return (field) -> true;
        }
        
        RoleBasedFieldAuthorization fieldAuthorization = config.getFieldAuthorization();

        if (fieldAuthorization == null) {
            throw new IllegalStateException("FLS is not initialized");
        }

        try {
            
            if (privilegesEvaluationContext.getSpecialPrivilegesEvaluationContext() != null && privilegesEvaluationContext.getSpecialPrivilegesEvaluationContext().getRolesConfig() != null) {
                SgDynamicConfiguration<Role> roles = privilegesEvaluationContext.getSpecialPrivilegesEvaluationContext().getRolesConfig();
                fieldAuthorization = new RoleBasedFieldAuthorization(roles, ImmutableSet.of(index));
            }
            
            FlsRule flsRule = fieldAuthorization.getFlsRule(privilegesEvaluationContext, index);
            return (field) -> flsRule.isAllowed(removeSuffix(field));
        } catch (PrivilegesEvaluationException e) {
            log.error("Error while evaluating FLS for index " + index, e);
            throw new RuntimeException("Error while evaluating FLS for index " + index, e);
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

    private static String removeSuffix(String field) {
        if (field != null && field.endsWith(KEYWORD)) {
            return field.substring(0, field.length() - KEYWORD.length());
        }
        return field;
    }

}
