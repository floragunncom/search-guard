package com.floragunn.searchguard.authtoken;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.floragunn.searchguard.authtoken.RequestedPrivileges.IndexPermissions;
import com.floragunn.searchguard.authtoken.RequestedPrivileges.TenantPermissions;
import com.floragunn.searchguard.resolver.IndexResolverReplacer.Resolved;
import com.floragunn.searchguard.sgconf.SgRoles;
import com.floragunn.searchguard.user.User;

public class RestrictedSgRoles extends SgRoles {
    
    private final SgRoles base;
    private final RequestedPrivileges restriction;
    private final Set<String> clusterPermissions;
    private List<IndexPermissions> indexPermissions;
    private List<TenantPermissions> tenantPermissions;
    private List<String> roles;
    RestrictedSgRoles(SgRoles base, RequestedPrivileges restriction) {
        this.base = base;
        this.restriction =  restriction;
        this.clusterPermissions = new HashSet<>(restriction.getClusterPermissions());
    }


    @Override
    public boolean impliesClusterPermissionPermission(String action0) {
        return base.impliesClusterPermissionPermission(action0) && this.clusterPermissions.contains(action0);
    }

    @Override
    public Set<String> getRoleNames() {
        Set<String> result = new HashSet<>(roles.size());
        Set<String> baseRoles = base.getRoleNames();
        
        for (String role : roles) {
            if (baseRoles.contains(role)) {
                result.add(role);
            }
        }
        
        return result;
    }

    @Override
    public Set<String> reduce(Resolved requestedResolved, User user, String[] strings, IndexNameExpressionResolver resolver,
            ClusterService clusterService) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean impliesTypePermGlobal(Resolved requestedResolved, User user, String[] allIndexPermsRequiredA, IndexNameExpressionResolver resolver,
            ClusterService clusterService) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean get(Resolved requestedResolved, User user, String[] allIndexPermsRequiredA, IndexNameExpressionResolver resolver,
            ClusterService clusterService) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Map<String, Set<String>> getMaskedFields(User user, IndexNameExpressionResolver resolver, ClusterService clusterService) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Tuple<Map<String, Set<String>>, Map<String, Set<String>>> getDlsFls(User user, IndexNameExpressionResolver resolver,
            ClusterService clusterService) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Set<String> getAllPermittedIndicesForKibana(Resolved resolved, User user, String[] actions, IndexNameExpressionResolver resolver,
            ClusterService cs) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SgRoles filter(Set<String> roles) {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        throw new RuntimeException("Not implemented");
    }


}
