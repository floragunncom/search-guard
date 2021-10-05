/*
 * Copyright 2020 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.authtoken;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.floragunn.searchguard.privileges.ActionRequestIntrospector;
import com.floragunn.searchguard.privileges.ActionRequestIntrospector.ActionRequestInfo;
import com.floragunn.searchguard.privileges.ActionRequestIntrospector.ResolvedIndices;
import com.floragunn.searchguard.sgconf.ConfigModel.ActionGroupResolver;
import com.floragunn.searchguard.sgconf.EvaluatedDlsFlsConfig;
import com.floragunn.searchguard.sgconf.SgRoles;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.util.ImmutableSet;
import com.google.common.collect.Sets;

public class RestrictedSgRoles extends SgRoles {

    private final SgRoles base;
    private final SgRoles restrictionSgRoles;
    private final RequestedPrivileges restriction;

    RestrictedSgRoles(SgRoles base, RequestedPrivileges restriction, ActionGroupResolver actionGroupResolver) {
        this.base = base;
        this.restriction = restriction;
        this.restrictionSgRoles = com.floragunn.searchguard.sgconf.ConfigModelV7.SgRoles.create(restriction.toRolesConfig(), actionGroupResolver);
    }

    @Override
    public boolean impliesClusterPermissionPermission(String action0) {
        return base.impliesClusterPermissionPermission(action0) && restrictionSgRoles.impliesClusterPermissionPermission(action0);
    }

    @Override
    public Set<String> getRoleNames() {
        if (restriction.getRoles() == null || restriction.getRoles().size() == 0) {
            return base.getRoleNames();
        }

        Set<String> result = new HashSet<>(restriction.getRoles().size());
        Set<String> baseRoles = base.getRoleNames();

        for (String role : restriction.getRoles()) {
            if (baseRoles.contains(role)) {
                result.add(role);
            }
        }

        return result;
    }

    @Override
    public ImmutableSet<String> reduce(ResolvedIndices requestedResolved, User user, Set<String> strings, IndexNameExpressionResolver resolver,
            ClusterService clusterService) {
        ImmutableSet<String> restrictedIndexes = restrictionSgRoles.reduce(requestedResolved, user, strings, resolver, clusterService);

        if (restrictedIndexes.isEmpty()) {
            // Don't calculate base indexes if we already know we will get an empty set
            return ImmutableSet.empty();
        }

        ImmutableSet<String> baseIndexes = base.reduce(requestedResolved, user, strings, resolver, clusterService);

        return baseIndexes.intersection(restrictedIndexes);
    }

    @Override
    public boolean impliesTypePermGlobal(ResolvedIndices requestedResolved, User user, Set<String> allIndexPermsRequiredA, IndexNameExpressionResolver resolver,
            ClusterService clusterService) {
        boolean restrictedPermission = restrictionSgRoles.impliesTypePermGlobal(requestedResolved, user, allIndexPermsRequiredA, resolver,
                clusterService);

        if (!restrictedPermission) {
            // Don't calculate base permission if we already know we will get an empty set
            return false;
        }

        boolean basePermission = base.impliesTypePermGlobal(requestedResolved, user, allIndexPermsRequiredA, resolver, clusterService);

        return restrictedPermission && basePermission;
    }
    
    @Override
    public Set<String> getClusterPermissions(User user) {
        return Sets.intersection(base.getClusterPermissions(user), restrictionSgRoles.getClusterPermissions(user));
    }

    @Override
    public boolean get(ResolvedIndices requestedResolved, User user, Set<String> allIndexPermsRequiredA, IndexNameExpressionResolver resolver,
            ClusterService clusterService) {
        boolean restrictedPermission = restrictionSgRoles.get(requestedResolved, user, allIndexPermsRequiredA, resolver, clusterService);

        if (!restrictedPermission) {
            // Don't calculate base permission if we already know we will get an empty set
            return false;
        }

        boolean basePermission = base.get(requestedResolved, user, allIndexPermsRequiredA, resolver, clusterService);

        return restrictedPermission && basePermission;
    }

    @Override
    public EvaluatedDlsFlsConfig getDlsFls(User user, IndexNameExpressionResolver resolver,
            ClusterService clusterService, NamedXContentRegistry namedXContentRegistry) {
        // not implemented
        return base.getDlsFls(user, resolver, clusterService, namedXContentRegistry);
    }

    @Override
    public ImmutableSet<String> getAllPermittedIndicesForKibana(ActionRequestInfo resolved, User user, Set<String> actions, IndexNameExpressionResolver resolver,
            ClusterService cs,  ActionRequestIntrospector actionRequestIntrospector) {
        ImmutableSet<String> restrictedIndexes = restrictionSgRoles.getAllPermittedIndicesForKibana(resolved, user, actions, resolver, cs, actionRequestIntrospector);

        if (restrictedIndexes.isEmpty()) {
            // Don't calculate base indexes if we already know we will get an empty set
            return ImmutableSet.empty();
        }

        ImmutableSet<String> baseIndexes = base.getAllPermittedIndicesForKibana(resolved, user, actions, resolver, cs, actionRequestIntrospector);

        return restrictedIndexes.intersection(baseIndexes);
    }

    @Override
    public SgRoles filter(Set<String> roles) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public TenantPermissions getTenantPermissions(User user, String requestedTenant) {
        TenantPermissions restricted = restrictionSgRoles.getTenantPermissions(user, requestedTenant);
        TenantPermissions base = this.base.getTenantPermissions(user, requestedTenant);

        return new TenantPermissions() {

            @Override
            public boolean isWritePermitted() {
                return restricted.isWritePermitted() && base.isWritePermitted();
            }

            @Override
            public boolean isReadPermitted() {
                return restricted.isReadPermitted() && base.isReadPermitted();
            }

            @Override
            public Set<String> getPermissions() {
                return Sets.intersection(restricted.getPermissions(), base.getPermissions());
            }
        };
    }

    @Override
    public boolean hasTenantPermission(User user, String requestedTenant, String action) {
        boolean restrictedPermission = restrictionSgRoles.hasTenantPermission(user, requestedTenant, action);
        
        if (!restrictedPermission) {
            return false;
        }
        
        boolean basePermission = base.hasTenantPermission(user, requestedTenant, action);
        
        return restrictedPermission && basePermission;
    }

    @Override
    public Map<String, Boolean> mapTenants(User user, Set<String> tenantNames) {
        Map<String, Boolean> restricted = restrictionSgRoles.mapTenants(user, tenantNames);
        Map<String, Boolean> base = this.base.mapTenants(user, tenantNames);
        
        HashMap<String, Boolean> result = new HashMap<>(base.size());
        
        for (Map.Entry<String, Boolean> entry : base.entrySet()) {
            Boolean restrictedBoolean = restricted.get(entry.getKey());
            
            if (restrictedBoolean != null) {
                result.put(entry.getKey(), restrictedBoolean.booleanValue() && entry.getValue());
            } 
        }

        return result;
    }

  

}
