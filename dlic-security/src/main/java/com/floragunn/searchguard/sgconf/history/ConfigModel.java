/*
 * Copyright 2022 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.sgconf.history;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;

import com.floragunn.searchguard.authc.blocking.Blocks;
import com.floragunn.searchguard.authz.ActionAuthorization;
import com.floragunn.searchguard.authz.DocumentAuthorization;
import com.floragunn.searchguard.authz.LegacyRoleBasedDocumentAuthorization;
import com.floragunn.searchguard.authz.RoleBasedActionAuthorization;
import com.floragunn.searchguard.authz.actions.Actions;
import com.floragunn.searchguard.authz.config.ActionGroup;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.authz.config.RoleMapping;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;

public class ConfigModel {
    private final SgDynamicConfiguration<Role> rolesConfig;
    private final SgDynamicConfiguration<RoleMapping> roleMappingConfig;
    private final SgDynamicConfiguration<ActionGroup> actionGroupsConfig;
    private final SgDynamicConfiguration<Tenant> tenantsConfig;
    private final SgDynamicConfiguration<Blocks> blocks;

    private final ActionGroup.FlattenedIndex actionGroups;
    private final ActionAuthorization actionAuthorization;
    private final DocumentAuthorization documentAuthorization;
    private final RoleMapping.InvertedIndex roleMapping;

    public ConfigModel(SgDynamicConfiguration<Role> roles, SgDynamicConfiguration<RoleMapping> roleMappingConfig,
            SgDynamicConfiguration<ActionGroup> actionGroupsConfig, SgDynamicConfiguration<Tenant> tenants,
            SgDynamicConfiguration<Blocks> blocks, Actions actions, Settings esSettings, IndexNameExpressionResolver resolver,
            ClusterService clusterService) {
        this.rolesConfig = roles;
        this.roleMappingConfig = roleMappingConfig;
        this.actionGroupsConfig = actionGroupsConfig;
        this.tenantsConfig = tenants;
        this.blocks = blocks;

        this.actionGroups = new ActionGroup.FlattenedIndex(actionGroupsConfig);

        this.actionAuthorization = new RoleBasedActionAuthorization(roles, actionGroups, actions, null, tenantsConfig.getCEntries().keySet());
        this.documentAuthorization = new LegacyRoleBasedDocumentAuthorization(roles, resolver, clusterService);

        this.roleMapping = new RoleMapping.InvertedIndex(roleMappingConfig);
    }

    public ConfigModel(ActionAuthorization actionAuthorization, DocumentAuthorization documentAuthorization, RoleMapping.InvertedIndex roleMapping,
            ActionGroup.FlattenedIndex actionGroups) {
        this.actionAuthorization = actionAuthorization;
        this.documentAuthorization = documentAuthorization;
        this.roleMapping = roleMapping;
        this.rolesConfig = null;
        this.roleMappingConfig = null;
        this.actionGroupsConfig = null;
        this.tenantsConfig = null;
        this.blocks = null;
        this.actionGroups = actionGroups;
    }

    public ActionAuthorization getActionAuthorization() {
        return actionAuthorization;
    }

    public DocumentAuthorization getDocumentAuthorization() {
        return documentAuthorization;
    }

    public RoleMapping.InvertedIndex getRoleMapping() {
        return roleMapping;
    }

    public ActionGroup.FlattenedIndex getActionGroups() {
        return actionGroups;
    }

    @Override
    public String toString() {
        return "ConfigModel [rolesConfig=" + rolesConfig + ", roleMappingConfig=" + roleMappingConfig + ", actionGroupsConfig=" + actionGroupsConfig
                + ", tenantsConfig=" + tenantsConfig + ", blocks=" + blocks + "]";
    }
}
