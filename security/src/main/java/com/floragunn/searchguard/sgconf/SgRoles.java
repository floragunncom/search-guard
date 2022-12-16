package com.floragunn.searchguard.sgconf;

import java.util.Map;
import java.util.Set;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;

import com.floragunn.searchguard.resolver.IndexResolverReplacer.Resolved;
import com.floragunn.searchguard.user.User;

public abstract class SgRoles implements ToXContentObject {

    public abstract boolean impliesClusterPermissionPermission(String action0);

    public abstract Set<String> getRoleNames();

    public abstract Set<String> reduce(Resolved requestedResolved, User user, String[] strings, IndexNameExpressionResolver resolver,
            ClusterService clusterService);

    public abstract boolean impliesTypePermGlobal(Resolved requestedResolved, User user, String[] allIndexPermsRequiredA, IndexNameExpressionResolver resolver,
            ClusterService clusterService);

    public abstract boolean get(Resolved requestedResolved, User user, String[] allIndexPermsRequiredA, IndexNameExpressionResolver resolver,
            ClusterService clusterService);

    public abstract EvaluatedDlsFlsConfig getDlsFls(User user, IndexNameExpressionResolver resolver,
            ClusterService clusterService, NamedXContentRegistry namedXContentRegistry);

    public abstract Set<String> getAllPermittedIndicesForKibana(Resolved resolved, User user, String[] actions, IndexNameExpressionResolver resolver, ClusterService cs);

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
    
    public abstract boolean isIndexPrivilegeAliasResolutionEnabled();

}
