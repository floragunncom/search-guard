/*
 * Copyright 2024 by floragunn GmbH - All rights reserved
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

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.elasticsearch.common.util.concurrent.ThreadContext;

import com.floragunn.codova.config.templates.Template;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchsupport.StaticSettings;
import com.google.common.hash.Hashing;

class ThreadContextAuthzHashProvider {
    static final String THREAD_CONTEXT_HEADER = "_sg_dls_fls_authz";

    private final boolean active;
    private final ThreadContext threadContext;

    ThreadContextAuthzHashProvider(StaticSettings staticSettings, ThreadContext threadContext) {
        this.active = staticSettings.get(DlsFlsModule.PROVIDE_THREAD_CONTEXT_AUTHZ_HASH);
        this.threadContext = threadContext;
    }

    void noRestrictions() {
        if (this.active && this.threadContext.getHeader(THREAD_CONTEXT_HEADER) == null) {
            this.threadContext.putHeader(THREAD_CONTEXT_HEADER, "0");
        }
    }

    void restrictions(PrivilegesEvaluationContext context, DlsFlsProcessedConfig config) {
        if (this.active && this.threadContext.getHeader(THREAD_CONTEXT_HEADER) == null) {
            String restrictionsInfo = restrictionsInfo(context, config);
            this.threadContext.putHeader(THREAD_CONTEXT_HEADER, Hashing.sha256().hashString(restrictionsInfo, StandardCharsets.UTF_8).toString());
        }

    }

    String restrictionsInfo(PrivilegesEvaluationContext context, DlsFlsProcessedConfig config) {
        SgDynamicConfiguration<Role> rolesConfig;

        if (context.getSpecialPrivilegesEvaluationContext() != null) {
            rolesConfig = context.getSpecialPrivilegesEvaluationContext().getRolesConfig();
        } else {
            rolesConfig = config.getRoleConfig();
        }

        return restrictionsInfo(context, rolesConfig);
    }

    String restrictionsInfo(PrivilegesEvaluationContext context, SgDynamicConfiguration<Role> rolesConfig) {
        if (usesTemplates(context, rolesConfig)) {
            return userBasedRestrictionsInfo(context);
        } else {
            return roleBasedRestrictionInfo(context, rolesConfig);
        }
    }

    boolean usesTemplates(PrivilegesEvaluationContext context, SgDynamicConfiguration<Role> rolesConfig) {
        for (String roleName : context.getMappedRoles()) {
            Role role = rolesConfig.getCEntry(roleName);

            if (role != null) {
                for (Role.Index indexPermissions : role.getIndexPermissions()) {
                    if (indexPermissions.usesTemplates()) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    String userBasedRestrictionsInfo(PrivilegesEvaluationContext context) {
        return context.getUser().getName() + "::" + DocNode.wrap(context.getUser().getStructuredAttributes()).toJsonString() + "::" + context.getMappedRoles();
    }

    String roleBasedRestrictionInfo(PrivilegesEvaluationContext context, SgDynamicConfiguration<Role> rolesConfig) {
        Set<String> unprotectedIndices = new TreeSet<>();
        Map<String, Set<String>> protectedIndices = new TreeMap<>();

        for (String roleName : context.getMappedRoles()) {
            Role role = rolesConfig.getCEntry(roleName);

            if (role != null) {
                for (Role.Index indexPermissions : role.getIndexPermissions()) {
                    if (indexPermissions.getDls() == null && (indexPermissions.getFls() == null || indexPermissions.getFls().isEmpty())
                            && (indexPermissions.getMaskedFields() == null || indexPermissions.getMaskedFields().isEmpty())) {
                        unprotectedIndices.addAll(indexPermissions.getIndexPatterns().getSource().map(Template::toString));
                    } else {
                        for (String indexPattern : indexPermissions.getIndexPatterns().getSource().map(Template::toString)) {
                            Set<String> rules = protectedIndices.computeIfAbsent(indexPattern, k -> new TreeSet<>());
                            rules.add("dls:" + indexPermissions.getDls());
                            rules.add("fls: " + indexPermissions.getFls());
                            rules.add("fm: " + indexPermissions.getMaskedFields());
                        }
                    }
                }
            }
        }

        for (String unprotectedIndex : unprotectedIndices) {
            protectedIndices.remove(unprotectedIndex);
        }

        return unprotectedIndices + "::" + protectedIndices;
    }
}
