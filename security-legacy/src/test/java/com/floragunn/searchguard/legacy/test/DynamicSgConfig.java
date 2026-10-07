/*
 * Copyright 2015-2017 floragunn GmbH
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

package com.floragunn.searchguard.legacy.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;

import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;

public class DynamicSgConfig {
    
    private String searchGuardIndexName = "searchguard";
    private String sgRoles = "sg_roles.yml";
    private String sgTenants = "sg_roles_tenants.yml";
    private String sgRolesMapping = "sg_roles_mapping.yml";
    private String sgInternalUsers = "sg_internal_users.yml";
    private String sgActionGroups = "sg_action_groups.yml";
    private String sgBlocks = "sg_blocks.yml";
    private String sgAuthc = "sg_authc.yml";
    private String type = "_doc";
    private String legacyConfigFolder = "";

    public String getSearchGuardIndexName() {
        return searchGuardIndexName;
    }
    public DynamicSgConfig setSearchGuardIndexName(String searchGuardIndexName) {
        this.searchGuardIndexName = searchGuardIndexName;
        return this;
    }

    public DynamicSgConfig setSgRoles(String sgRoles) {
        this.sgRoles = sgRoles;
        return this;
    }

    public DynamicSgConfig setSgRolesMapping(String sgRolesMapping) {
        this.sgRolesMapping = sgRolesMapping;
        return this;
    }

    public DynamicSgConfig setSgInternalUsers(String sgInternalUsers) {
        this.sgInternalUsers = sgInternalUsers;
        return this;
    }

    public DynamicSgConfig setSgActionGroups(String sgActionGroups) {
        this.sgActionGroups = sgActionGroups;
        return this;
    }
    public DynamicSgConfig setSgTenants(String sgTenants) {
        this.sgTenants = sgTenants;
        return this;
    }
    
    public DynamicSgConfig setSgBlocks(String sgBlocks) {
        this.sgBlocks = sgBlocks;
        return this;
    }

    public String getType() {
        return type;
    }

    public List<IndexRequest> getDynamicConfig(String folder) {
        
        final String prefix = legacyConfigFolder+(folder == null?"":folder+"/");
        
        List<IndexRequest> ret = new ArrayList<>();
        
        ret.add(new IndexRequest(searchGuardIndexName)
        .id(CType.AUTHC.toLCString())
        .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
        .source(CType.AUTHC.toLCString(), FileHelper.readYamlContent(prefix+sgAuthc)));
        
        ret.add(new IndexRequest(searchGuardIndexName)
        .id(CType.ACTIONGROUPS.toLCString())
        .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
        .source(CType.ACTIONGROUPS.toLCString(), FileHelper.readYamlContent(prefix+sgActionGroups)));
 
        ret.add(new IndexRequest(searchGuardIndexName)
        .id(CType.INTERNALUSERS.toLCString())
        .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
        .source(CType.INTERNALUSERS.toLCString(), FileHelper.readYamlContent(prefix+sgInternalUsers)));
 
        ret.add(new IndexRequest(searchGuardIndexName)
        .id(CType.ROLES.toLCString())
        .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
        .source(CType.ROLES.toLCString(), FileHelper.readYamlContent(prefix+sgRoles)));
 
        ret.add(new IndexRequest(searchGuardIndexName)
        .id(CType.ROLESMAPPING.toLCString())
        .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
        .source(CType.ROLESMAPPING.toLCString(), FileHelper.readYamlContent(prefix+sgRolesMapping)));

        if("".equals(legacyConfigFolder)) {
            ret.add(new IndexRequest(searchGuardIndexName)
            .id(CType.TENANTS.toLCString())
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .source(CType.TENANTS.toLCString(), FileHelper.readYamlContent(prefix+sgTenants)));

            ret.add(new IndexRequest(searchGuardIndexName)
                    .id(CType.BLOCKS.toLCString())
                    .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source(CType.BLOCKS.toLCString(), FileHelper.readYamlContent(prefix+sgBlocks)));
        }
        
        return Collections.unmodifiableList(ret);
    }
}
