package com.floragunn.searchguard.sgconf;

import java.util.Map;
import java.util.Set;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContentObject;

import com.floragunn.searchguard.privileges.ActionRequestIntrospector;
import com.floragunn.searchguard.privileges.ActionRequestIntrospector.ActionRequestInfo;
import com.floragunn.searchguard.privileges.ActionRequestIntrospector.ResolvedIndices;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.util.ImmutableSet;

public abstract class SgRoles implements ToXContentObject {

    public abstract boolean impliesClusterPermissionPermission(String action0);

    public abstract Set<String> getRoleNames();

    public abstract ImmutableSet<String> reduce(ResolvedIndices requestedResolved, User user, Set<String> requiredPrivileges, IndexNameExpressionResolver resolver,
            ClusterService clusterService);

    public abstract boolean impliesTypePermGlobal(ResolvedIndices requestedResolved, User user, Set<String> requiredPrivileges,
            IndexNameExpressionResolver resolver, ClusterService clusterService);

    public abstract boolean get(ResolvedIndices requestedResolved, User user, Set<String> requiredPrivileges, IndexNameExpressionResolver resolver,
            ClusterService clusterService);

    public abstract EvaluatedDlsFlsConfig getDlsFls(User user, IndexNameExpressionResolver resolver,
            ClusterService clusterService, NamedXContentRegistry namedXContentRegistry);

    public abstract ImmutableSet<String> getAllPermittedIndicesForKibana(ActionRequestInfo requestInfo, User user, Set<String> actions,
            IndexNameExpressionResolver resolver, ClusterService cs, ActionRequestIntrospector actionRequestIntrospector);

    public abstract SgRoles filter(Set<String> roles);

    public abstract TenantPermissions getTenantPermissions(User user, String requestedTenant);
    
    public abstract Set<String> getClusterPermissions(User user);
    
    public abstract boolean hasTenantPermission(User user, String requestedTenant, String action);
    
    /**
     * Only used for authinfo REST API
     */
    public abstract Map<String, Boolean> mapTenants(User user, Set<String> allTenantNames);
    
    public interface TenantPermissions {
        public boolean isReadPermitted();
        public boolean isWritePermitted();
        public Set<String> getPermissions();
    }
    
    
}
