package com.floragunn.searchguard.sgconf;

import java.util.Map;
import java.util.Set;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
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

    public abstract Map<String, Set<String>> getMaskedFields(User user, IndexNameExpressionResolver resolver, ClusterService clusterService);

    public abstract Tuple<Map<String, Set<String>>, Map<String, Set<String>>> getDlsFls(User user, IndexNameExpressionResolver resolver,
            ClusterService clusterService);

    public abstract Set<String> getAllPermittedIndicesForKibana(Resolved resolved, User user, String[] actions, IndexNameExpressionResolver resolver, ClusterService cs);

    public abstract SgRoles filter(Set<String> roles);

}
