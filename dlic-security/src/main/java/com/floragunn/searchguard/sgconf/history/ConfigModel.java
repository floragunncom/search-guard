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

import org.elasticsearch.common.settings.Settings;

import com.floragunn.searchguard.authz.ActionAuthorization;
import com.floragunn.searchguard.authz.Actions;
import com.floragunn.searchguard.authz.DocumentAuthorization;
import com.floragunn.searchguard.authz.Role;
import com.floragunn.searchguard.authz.RoleBasedActionAuthorization;
import com.floragunn.searchguard.authz.RoleBasedDocumentAuthorization;
import com.floragunn.searchguard.authz.RoleMapping;
import com.floragunn.searchguard.sgconf.ActionGroups;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.sgconf.impl.v7.ActionGroupsV7;
import com.floragunn.searchguard.sgconf.impl.v7.BlocksV7;
import com.floragunn.searchguard.sgconf.impl.v7.TenantV7;

public class ConfigModel {
    private final SgDynamicConfiguration<Role> rolesConfig;
    private final SgDynamicConfiguration<RoleMapping> roleMappingConfig;
    private final SgDynamicConfiguration<ActionGroupsV7> actionGroupsConfig;
    private final SgDynamicConfiguration<TenantV7> tenantsConfig;
    private final SgDynamicConfiguration<BlocksV7> blocks;

    private final ActionGroups actionGroups;
    private final ActionAuthorization actionAuthorization;
    private final DocumentAuthorization documentAuthorization;
    private final RoleMapping.InvertedIndex roleMapping;

    public ConfigModel(SgDynamicConfiguration<Role> roles, SgDynamicConfiguration<RoleMapping> roleMappingConfig,
            SgDynamicConfiguration<ActionGroupsV7> actionGroupsConfig, SgDynamicConfiguration<TenantV7> tenants,
            SgDynamicConfiguration<BlocksV7> blocks, Actions actions, Settings esSettings) {
        this.rolesConfig = roles;
        this.roleMappingConfig = roleMappingConfig;
        this.actionGroupsConfig = actionGroupsConfig;
        this.tenantsConfig = tenants;
        this.blocks = blocks;

        this.actionGroups = new ActionGroups(actionGroupsConfig);

        actionAuthorization = new RoleBasedActionAuthorization(roles, actionGroups, actions, null, tenantsConfig.getCEntries().keySet());

        documentAuthorization = new RoleBasedDocumentAuthorization(roles, actionGroups, actions, null);

        roleMapping = new RoleMapping.InvertedIndex(roleMappingConfig);
    }

    public ConfigModel(ActionAuthorization actionAuthorization, DocumentAuthorization documentAuthorization, RoleMapping.InvertedIndex roleMapping,
            ActionGroups actionGroups) {
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

    public ActionGroups getActionGroups() {
        return actionGroups;
    }
}
