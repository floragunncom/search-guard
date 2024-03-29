/*
 * Copyright 2017-2022 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.femt;

import static com.floragunn.searchguard.authz.SyncAuthorizationFilter.Result.DENIED;
import static com.floragunn.searchguard.authz.SyncAuthorizationFilter.Result.INTERCEPTED;
import static com.floragunn.searchguard.authz.SyncAuthorizationFilter.Result.OK;
import static com.floragunn.searchguard.privileges.PrivilegesInterceptor.InterceptionResult.ALLOW;
import static com.floragunn.searchguard.privileges.PrivilegesInterceptor.InterceptionResult.DENY;
import static com.floragunn.searchguard.privileges.PrivilegesInterceptor.InterceptionResult.NORMAL;

import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.ActionAuthorization;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.actions.Action;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector.ResolvedIndices;
import com.floragunn.searchguard.authz.actions.Actions;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.privileges.PrivilegesInterceptor;
import com.floragunn.searchguard.user.User;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.floragunn.searchguard.authz.SyncAuthorizationFilter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.IndicesRequest.Replaceable;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetRequest.Item;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;

public class PrivilegesInterceptorImpl implements PrivilegesInterceptor {

    private static final String USER_TENANT = "__user__";
    private static final ImmutableSet<String> READ_ONLY_ALLOWED_ACTIONS = ImmutableSet.of(
        "indices:admin/get",
        "indices:data/read/get",
        "indices:data/read/search",
        "indices:data/read/msearch",
        "indices:data/read/mget",
        "indices:data/read/mget[shard]"
    );

    private final Action KIBANA_ALL_SAVED_OBJECTS_WRITE;
    private final Action KIBANA_ALL_SAVED_OBJECTS_READ;

    protected final Logger log = LogManager.getLogger(this.getClass());
    private final String kibanaServerUsername;
    private final String kibanaIndexName;
    private final String kibanaIndexNamePrefix;
    private final Pattern versionedKibanaIndexPattern;
    private final Pattern kibanaIndexPatternWithTenant;
    private final ImmutableSet<String> tenantNames;
    private final boolean enabled;
    private final ClusterService clusterService;
    private final IndicesService indicesService;

    public PrivilegesInterceptorImpl(FeMultiTenancyConfig config, ImmutableSet<String> tenantNames, Actions actions, ClusterService clusterService, IndicesService indicesService) {
        this.enabled = config.isEnabled();
        this.kibanaServerUsername = config.getServerUsername();
        this.kibanaIndexName = config.getIndex();
        this.kibanaIndexNamePrefix = this.kibanaIndexName + "_";
        this.versionedKibanaIndexPattern = Pattern
                .compile(Pattern.quote(this.kibanaIndexName) + "(_-?[0-9]+_[a-z0-9]+)?(_[0-9]+\\.[0-9]+\\.[0-9]+(_[0-9]{3})?)");
        this.kibanaIndexPatternWithTenant = Pattern.compile(Pattern.quote(this.kibanaIndexName) + "(_-?[0-9]+_[a-z0-9]+(_[0-9]{3})?)");

        this.tenantNames = tenantNames.with(Tenant.GLOBAL_TENANT_ID);
        this.KIBANA_ALL_SAVED_OBJECTS_WRITE = actions.get("kibana:saved_objects/_/write");
        this.KIBANA_ALL_SAVED_OBJECTS_READ = actions.get("kibana:saved_objects/_/read");
        this.clusterService = Objects.requireNonNull(clusterService, "Cluster services are required");
        this.indicesService = Objects.requireNonNull(indicesService, "Indices service is required");
    }

    private boolean isTenantAllowed(PrivilegesEvaluationContext context, ActionRequest request, Action action, String requestedTenant,
            ActionAuthorization actionAuthorization) throws PrivilegesEvaluationException {

        if (!isTenantValid(requestedTenant)) {
            log.warn("Invalid tenant: " + requestedTenant + "; user: " + context.getUser());

            return false;
        }

        if (requestedTenant.equals(USER_TENANT)) {
            return true;
        }

        boolean hasReadPermission = actionAuthorization.hasTenantPermission(context, KIBANA_ALL_SAVED_OBJECTS_READ, requestedTenant).isOk();
        boolean hasWritePermission = actionAuthorization.hasTenantPermission(context, KIBANA_ALL_SAVED_OBJECTS_WRITE, requestedTenant).isOk();

        hasReadPermission |= hasWritePermission;

        if (!hasReadPermission) {
            log.warn("Tenant {} is not allowed for user {}", requestedTenant, context.getUser().getName());
            return false;
        }

        if (!hasWritePermission && !READ_ONLY_ALLOWED_ACTIONS.contains(action.name())) {
            log.warn("Tenant {} is not allowed to write (user: {})", requestedTenant, context.getUser().getName());
            return false;
        }

        return true;
    }

    @Override
    public InterceptionResult replaceKibanaIndex(
            PrivilegesEvaluationContext context, ActionRequest request, Action action, ActionAuthorization actionAuthorization, ActionListener<?> listener) throws PrivilegesEvaluationException {

        if (!enabled) {
            return NORMAL;
        }

        User user = context.getUser();
        ResolvedIndices requestedResolved = context.getRequestInfo().getResolvedIndices();
        
        if (user.getName().equals(kibanaServerUsername)) {
            return NORMAL;
        }

        if (log.isDebugEnabled()) {
            log.debug("replaceKibanaIndex(" + action + ", " + user + ")\nrequestedResolved: " + requestedResolved + "\nrequestedTenant: "
                    + user.getRequestedTenant());
        }

        IndexInfo kibanaIndexInfo = checkForExclusivelyUsedKibanaIndexOrAlias(request, requestedResolved);

        if (kibanaIndexInfo == null) {
            // This is not about the .kibana index: Nothing to do here, get out early!
            return NORMAL;
        }

        if (log.isDebugEnabled()) {
            log.debug("IndexInfo: " + kibanaIndexInfo);
        }

        String requestedTenant = user.getRequestedTenant();
        if (requestedTenant == null || requestedTenant.isEmpty()) {
            return NORMAL;
        }

        boolean frontendServerUser = user.getName().equals(kibanaServerUsername);
        TenantAccess tenantAccess = frontendServerUser ? TenantAccess.FULL_ACCESS : getTenantAccess(context, requestedTenant, actionAuthorization);

        SyncAuthorizationFilter.Result result = handleRequestRequiringSpecialTreatment(tenantAccess, context, (ActionListener<BulkResponse>) listener);
        if(result == INTERCEPTED) {
            return InterceptionResult.INTERCEPTED;
        }

        if (requestedTenant == null || requestedTenant.length() == 0 || requestedTenant.equals(Tenant.GLOBAL_TENANT_ID)) {
            if (kibanaIndexInfo.tenantInfoPart != null) {
                // XXX This indicates that the user tried to directly address an internal Kibana index including tenant name  (like .kibana_92668751_admin)
                // The original implementation allows these requests to pass with normal privileges if the sgtenant header is null. Tenant privileges are ignored then.
                // Integration tests (such as test_multitenancy_mget) are relying on this behaviour.
                return NORMAL;
            } else if (isTenantAllowed(context, request, action, Tenant.GLOBAL_TENANT_ID, actionAuthorization)) {
                return NORMAL;
            } else {
                return DENY;
            }
        } else {
            if (isTenantAllowed(context, request, action, requestedTenant, actionAuthorization)) {
                if (kibanaIndexInfo.isReplacementNeeded()) {
                    replaceIndex(request, kibanaIndexInfo.originalName, kibanaIndexInfo.toInternalIndexName(user), action, user);
                }
                return ALLOW;
            } else {
                return DENY;
            }
        }

        // TODO handle user tenant in that way that this tenant cannot be specified as
        // regular tenant
        // to avoid security issue

    }

    record TenantAccess(boolean hasReadPermission, boolean hasWritePermission) {

        public static final TenantAccess INACCESSIBLE = new TenantAccess(false, false);
        public static final TenantAccess FULL_ACCESS = new TenantAccess(true, true);

        public boolean hasAnyAccess() {
            return hasReadPermission | hasWritePermission;
        }

        public boolean isProhibited() {
            return ! hasAnyAccess();
        }

        public boolean isWriteProhibited() {
            return ! hasWritePermission;
        }

        public boolean isReadOnly() {
            return (!hasWritePermission) && hasReadPermission;
        }
    }

    /**
     * Method handle update request issued by search frontend when the dashboard is opened. User with read-only permission is not allowed
     * to perform the update. When SG returns 403 response then process of opening the dashboard is interrupted. Therefore, the method
     * replace response with code 403 to 404. Loading dashboard works correctly in case of 404 response (inside bulk request).
     */
    private SyncAuthorizationFilter.Result handleRequestRequiringSpecialTreatment(TenantAccess tenantAccess, PrivilegesEvaluationContext context, ActionListener<BulkResponse> listener) {
        boolean shouldIntercept = tenantAccess.isReadOnly() &&
                "indices:data/write/bulk".equals(context.getAction().name()) &&
                (context.getRequest() instanceof BulkRequest) &&
                isUpdateRequestDuringLoadingDashboard((BulkRequest) context.getRequest());
        if (shouldIntercept) {
            BulkRequest request = (BulkRequest) context.getRequest();
            notFoundBulkResponseForOnlyBulkRequest(listener, request);
            log.debug("Bulk only request permitted to load frontend dashboard.");
            return INTERCEPTED;
        }
        return DENIED;
    }

    private void notFoundBulkResponseForOnlyBulkRequest(ActionListener<BulkResponse> listener, BulkRequest request) {
        // intercepted bulk request is related only to one index
        String indexOrAlias = request.getIndices().stream().findFirst().orElseThrow();
        DocWriteRequest<?> firstBulkRequest = request.requests().get(0);
        ActionListener<BulkResponse> bulkListener = listener;
        BulkItemResponse[] items = new BulkItemResponse[1];
        ShardId firstPrimaryShardId = Optional.of(clusterService.state().getMetadata().getIndicesLookup()) //
                .map(lookup -> lookup.get(indexOrAlias)) //
                .map(IndexAbstraction::getWriteIndex) //
                .map(Index::getName) //
                .map(realIndexName -> clusterService.state().getMetadata().indices().get(realIndexName)) //
                .map(IndexMetadata::getIndex) //
                .map(indicesService::indexService) //
                .stream() //
                .flatMap(indexService -> indexService.shardIds().stream().map(shardId -> indexService.getShard(shardId))) //
                .filter(shard -> shard.routingEntry().primary()) //
                .map(AbstractIndexShardComponent::shardId) //
                .findFirst() //
                .orElse(null);

        log.debug("Found primary shard id '{}'", firstPrimaryShardId);

        DocumentMissingException documentMissingException = new DocumentMissingException(firstPrimaryShardId, indexOrAlias);
        BulkItemResponse.Failure failure = new BulkItemResponse.Failure(indexOrAlias, firstBulkRequest.id(), documentMissingException);
        int requestIndexInBulk = 0; // request index in bulk is used as id also here
        // org.elasticsearch.action.bulk.TransportBulkAction.BulkOperation.addFailure
        items[0] = BulkItemResponse.failure(requestIndexInBulk, DocWriteRequest.OpType.UPDATE, failure);
        bulkListener.onResponse(new BulkResponse(items, 1L));
    }

    private boolean isUpdateRequestDuringLoadingDashboard(BulkRequest request) {
        if(request.getIndices().size() != 1) {
            return false;
        }
        if(!new ArrayList<>(request.getIndices()).get(0).startsWith(kibanaIndexName)) {
            return false;
        }
        if(request.requests() == null || (request.requests().size() != 1)) {
            return false;
        }
        DocWriteRequest<?> firstBulkRequest = request.requests().get(0);
        if((firstBulkRequest.id() == null) || (!firstBulkRequest.id().startsWith("legacy-url-alias"))) {
            return false;
        }
        if(!(firstBulkRequest instanceof UpdateRequest updateRequest)) {
            return false;
        }
        Script script = updateRequest.script();
        if(script == null) {
            return false;
        }
        if(!"painless".contains(script.getLang())) {
            return false;
        }
        Map<String, Object> params = script.getParams();
        if((params == null) || (!params.containsKey("type") || (!params.containsKey("time")))) {
            return false;
        }
        if(!"legacy-url-alias".equals(params.get("type"))) {
            return false;
        }
        String code = script.getIdOrCode();
        if((code == null) || (!code.contains("ctx._source[params.type].disabled")) || (!code.contains("ctx._source[params.type].resolveCounter"))) {
            return false;
        }
        return true;
    }

    private TenantAccess getTenantAccess(PrivilegesEvaluationContext context, String requestedTenant, ActionAuthorization actionAuthorization) throws PrivilegesEvaluationException {
        if (!Tenant.GLOBAL_TENANT_ID.equals(requestedTenant) && !USER_TENANT.equals(requestedTenant) && !tenantNames.contains(requestedTenant)) {
            log.warn("Invalid tenant: " + requestedTenant + "; user: " + context.getUser());

            return TenantAccess.INACCESSIBLE;
        }

        if (USER_TENANT.equals(requestedTenant)) {
            return TenantAccess.FULL_ACCESS;
        }

        return new TenantAccess(
                actionAuthorization.hasTenantPermission(context, KIBANA_ALL_SAVED_OBJECTS_READ, requestedTenant).isOk(),
                actionAuthorization.hasTenantPermission(context, KIBANA_ALL_SAVED_OBJECTS_WRITE, requestedTenant).isOk()
        );
    }

    private void replaceIndex(ActionRequest request, String oldIndexName, String newIndexName, Action action, User user) {
        boolean kibOk = false;

        if (log.isDebugEnabled()) {
            log.debug("{} index will be replaced with {} in this {} request", oldIndexName, newIndexName, request.getClass().getName());
        }

        if (request instanceof GetFieldMappingsIndexRequest || request instanceof GetFieldMappingsRequest) {
            return;
        }

        //handle msearch and mget
        //in case of GET change the .kibana index to the userskibanaindex
        //in case of Search add the userskibanaindex
        //if (request instanceof CompositeIndicesRequest) {
        String[] newIndexNames = new String[] { newIndexName };

        if (request instanceof CreateIndexRequest) {
            ((CreateIndexRequest) request).index(newIndexName);
            kibOk = true;
        } else if (request instanceof BulkRequest) {

            for (DocWriteRequest<?> ar : ((BulkRequest) request).requests()) {

                if (ar instanceof DeleteRequest) {
                    ((DeleteRequest) ar).index(newIndexName);
                }

                if (ar instanceof IndexRequest) {
                    ((IndexRequest) ar).index(newIndexName);
                }

                if (ar instanceof UpdateRequest) {
                    ((UpdateRequest) ar).index(newIndexName);
                }
            }

            kibOk = true;

        } else if (request instanceof MultiGetRequest) {

            for (Item item : ((MultiGetRequest) request).getItems()) {
                item.index(newIndexName);
            }

            kibOk = true;

        } else if (request instanceof MultiSearchRequest) {

            for (SearchRequest ar : ((MultiSearchRequest) request).requests()) {
                ar.indices(newIndexNames);
            }

            kibOk = true;

        } else if (request instanceof MultiTermVectorsRequest) {

            for (TermVectorsRequest ar : (Iterable<TermVectorsRequest>) () -> ((MultiTermVectorsRequest) request).iterator()) {
                ar.index(newIndexName);
            }

            kibOk = true;
        } else if (request instanceof UpdateRequest) {
            ((UpdateRequest) request).index(newIndexName);
            kibOk = true;
        } else if (request instanceof IndexRequest) {
            ((IndexRequest) request).index(newIndexName);
            kibOk = true;
        } else if (request instanceof DeleteRequest) {
            ((DeleteRequest) request).index(newIndexName);
            kibOk = true;
        } else if (request instanceof SingleShardRequest) {
            ((SingleShardRequest<?>) request).index(newIndexName);
            kibOk = true;
        } else if (request instanceof RefreshRequest) {
            ((RefreshRequest) request).indices(newIndexNames); //???
            kibOk = true;
        } else if (request instanceof ReplicationRequest) {
            ((ReplicationRequest<?>) request).index(newIndexName);
            kibOk = true;
        } else if (request instanceof Replaceable) {
            Replaceable replaceableRequest = (Replaceable) request;
            replaceableRequest.indices(newIndexNames);
            kibOk = true;
        } else if (request instanceof IndicesAliasesRequest) {
            IndicesAliasesRequest indicesAliasesRequest = (IndicesAliasesRequest) request;

            for (AliasActions aliasActions : indicesAliasesRequest.getAliasActions()) {
                if (aliasActions.indices() == null || aliasActions.indices().length != 1) {
                    // This is guarded by replaceKibanaIndex()
                    // Only actions operating on a single Kibana index should arrive here.
                    log.warn("Unexpected AliasActions: " + aliasActions);
                    continue;
                }

                if (!aliasActions.indices()[0].equals(oldIndexName)) {
                    // This is guarded by replaceKibanaIndex()
                    // Only actions operating on a single Kibana index should arrive here.
                    log.warn("Unexpected AliasActions: " + aliasActions + "; expected index: " + oldIndexName);
                    continue;
                }

                aliasActions.index(newIndexName);

                if (aliasActions.actionType() != AliasActions.Type.REMOVE_INDEX) {
                    if (aliasActions.aliases() == null) {
                        log.warn("Unexpected AliasActions: " + aliasActions);
                        continue;
                    }

                    String[] aliases = aliasActions.aliases();
                    String[] newAliases = new String[aliases.length];
                    for (int i = 0; i < aliases.length; i++) {
                        IndexInfo indexInfo = checkForExclusivelyUsedKibanaIndexOrAlias(aliases[i]);

                        if (indexInfo != null && indexInfo.isReplacementNeeded()) {
                            newAliases[i] = indexInfo.toInternalIndexName(user);
                        } else {
                            newAliases[i] = aliases[i];
                        }
                    }

                    aliasActions.aliases(newAliases);
                }

                if (log.isDebugEnabled()) {
                    log.debug("Rewritten IndicesAliasesRequest: " + indicesAliasesRequest.getAliasActions());
                }
            }

            kibOk = true;
        } else {
            log.warn("Dont know what to do (1) with {}", request.getClass());
        }

        if (!kibOk) {
            log.warn("Dont know what to do (2) with {}", request.getClass());
        }
    }

    private IndexInfo checkForExclusivelyUsedKibanaIndexOrAlias(ActionRequest request, ResolvedIndices requestedResolved) {
        if (requestedResolved.isLocalAll()) {
            return null;
        }

        Set<String> indices = getIndices(request);

        if (indices.size() == 1) {
            return checkForExclusivelyUsedKibanaIndexOrAlias(indices.iterator().next());
        } else {
            return null;
        }
    }

    private IndexInfo checkForExclusivelyUsedKibanaIndexOrAlias(String aliasOrIndex) {

        if (aliasOrIndex.equals(kibanaIndexName)) {
            // Pre 7.12: Just .kibana
            return new IndexInfo(aliasOrIndex, kibanaIndexName, null);
        }

        if (aliasOrIndex.startsWith(kibanaIndexNamePrefix)) {
            Matcher matcher = versionedKibanaIndexPattern.matcher(aliasOrIndex);

            if (matcher.matches()) {
                // Post 7.12: .kibana_7.12.0
                // Prefix will be: .kibana_
                // Suffix will be: _7.12.0
                if (matcher.group(1) == null) {
                    // Basic case
                    return new IndexInfo(aliasOrIndex, kibanaIndexName, matcher.group(2));
                } else {
                    // We have here the case that the index replacement has been already applied.
                    // This can happen when internal ES operations trigger further operations, e.g. an index triggers an auto_create.
                    return new IndexInfo(aliasOrIndex, kibanaIndexName, matcher.group(2), matcher.group(1));
                }
            }

            matcher = kibanaIndexPatternWithTenant.matcher(aliasOrIndex);

            if (matcher.matches()) {
                // Pre 7.12: .kibana_12345678_tenantname

                return new IndexInfo(aliasOrIndex, kibanaIndexName, null, matcher.group(1));
            }
        }

        return null;
    }

    private Set<String> getIndices(ActionRequest request) {
        if (request instanceof PutMappingRequest) {
            PutMappingRequest putMappingRequest = (PutMappingRequest) request;

            return ImmutableSet.of(putMappingRequest.getConcreteIndex() != null ? putMappingRequest.getConcreteIndex().getName() : null,
                    putMappingRequest.indices());
        } else if (request instanceof IndicesRequest) {
            if (((IndicesRequest) request).indices() != null) {
                return ImmutableSet.of(Arrays.asList(((IndicesRequest) request).indices()));
            } else {
                return Collections.emptySet();
            }
        } else if (request instanceof BulkRequest) {
            return ((BulkRequest) request).getIndices();
        } else if (request instanceof MultiGetRequest) {
            return ((MultiGetRequest) request).getItems().stream().map(item -> item.index()).collect(Collectors.toSet());
        } else if (request instanceof MultiSearchRequest) {
            return ((MultiSearchRequest) request).requests().stream().flatMap(r -> Arrays.stream(r.indices())).collect(Collectors.toSet());
        } else if (request instanceof MultiTermVectorsRequest) {
            return ((MultiTermVectorsRequest) request).getRequests().stream().flatMap(r -> Arrays.stream(r.indices())).collect(Collectors.toSet());
        } else {
            if (log.isTraceEnabled()) {
                log.trace("Not supported for multi tenancy: " + request);
            }

            return Collections.emptySet();
        }
    }

    private class IndexInfo {
        final String originalName;
        final String prefix;
        final String suffix;
        final String tenantInfoPart;

        IndexInfo(String originalName, String prefix, String suffix) {
            this.originalName = originalName;
            this.prefix = prefix;
            this.suffix = suffix;
            this.tenantInfoPart = null;
        }

        IndexInfo(String originalName, String prefix, String suffix, String tenantInfoPart) {
            this.originalName = originalName;
            this.prefix = prefix;
            this.suffix = suffix;
            this.tenantInfoPart = tenantInfoPart;
        }

        String toInternalIndexName(User user) {
            if (USER_TENANT.equals(user.getRequestedTenant())) {
                return toInternalIndexName(user.getName());
            } else {
                return toInternalIndexName(user.getRequestedTenant());
            }
        }

        boolean isReplacementNeeded() {
            return this.tenantInfoPart == null;
        }

        private String toInternalIndexName(String tenant) {
            if (tenant == null) {
                throw new ElasticsearchException("tenant must not be null here");
            }

            String tenantInfoPart = "_" + tenant.hashCode() + "_" + tenant.toLowerCase().replaceAll("[^a-z0-9]+", "");

            if (this.tenantInfoPart != null && !this.tenantInfoPart.equals(tenantInfoPart)) {
                throw new ElasticsearchSecurityException(
                        "This combination of sgtenant header and index is not allowed.\nTenant: " + tenant + "\nIndex: " + originalName,
                        RestStatus.BAD_REQUEST);
            }

            StringBuilder result = new StringBuilder(prefix).append(tenantInfoPart);

            if (this.suffix != null) {
                result.append(suffix);
            }

            return result.toString();
        }

        @Override
        public String toString() {
            return "IndexInfo [originalName=" + originalName + ", prefix=" + prefix + ", suffix=" + suffix + ", tenantInfoPart=" + tenantInfoPart
                    + "]";
        }

    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public String getKibanaIndex() {
        return kibanaIndexName;
    }

    @Override
    public String getKibanaServerUser() {
        return kibanaServerUsername;
    }

    @Override
    public Map<String, Boolean> mapTenants(User user, ImmutableSet<String> roles, ActionAuthorization actionAuthorization) {
        if (user == null) {
            return ImmutableMap.empty();
        }

        ImmutableMap.Builder<String, Boolean> result = new ImmutableMap.Builder<>(roles.size());
        result.put(user.getName(), true);

        PrivilegesEvaluationContext context = new PrivilegesEvaluationContext(user, roles, null, null, false, null, null);
        
        for (String tenant : this.tenantNames) {
            try {
                boolean hasReadPermission = actionAuthorization.hasTenantPermission(context, KIBANA_ALL_SAVED_OBJECTS_READ, tenant).isOk();
                boolean hasWritePermission = actionAuthorization.hasTenantPermission(context, KIBANA_ALL_SAVED_OBJECTS_WRITE, tenant).isOk();

                if (hasWritePermission) {
                    result.put(tenant, true);
                } else if (hasReadPermission) {
                    result.put(tenant, false);
                }
            } catch (PrivilegesEvaluationException e) {
                log.error("Error while evaluating privileges for " + user + " " + tenant, e);
            }
        }

        return result.build();
    }

    private boolean isTenantValid(String tenant) {
        return User.USER_TENANT.equals(tenant) || tenantNames.contains(tenant);
    }

}
