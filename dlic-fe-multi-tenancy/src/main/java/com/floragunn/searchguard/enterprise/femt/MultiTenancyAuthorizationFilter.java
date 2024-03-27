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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.floragunn.searchguard.authz.TenantManager;
import com.floragunn.searchguard.enterprise.femt.request.handler.RequestHandler;
import com.floragunn.searchguard.enterprise.femt.request.handler.RequestHandlerFactory;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.SyncAuthorizationFilter;
import com.floragunn.searchguard.authz.actions.Action;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector.ResolvedIndices;
import com.floragunn.searchguard.authz.actions.Actions;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.Script;

public class MultiTenancyAuthorizationFilter implements SyncAuthorizationFilter {

    static final String TEMP_MIGRATION_INDEX_NAME_POSTFIX_1 = "_reindex_temp";
    static final String TEMP_MIGRATION_INDEX_NAME_POSTFIX_2 = "_reindex_temp_alias";

    public static final String SG_FILTER_LEVEL_FEMT_DONE = ConfigConstants.SG_CONFIG_PREFIX + "filter_level_femt_done";

    private static final ImmutableSet<String> READ_ONLY_ALLOWED_ACTIONS = ImmutableSet.of(
                "indices:admin/get",
                "indices:data/read/get",
                "indices:data/read/search",
                "indices:data/read/msearch",
                "indices:data/read/mget",
                "indices:data/read/mget[shard]",
                "indices:data/read/open_point_in_time");

    private final Action KIBANA_ALL_SAVED_OBJECTS_WRITE;
    private final Action KIBANA_ALL_SAVED_OBJECTS_READ;

    protected final Logger log = LogManager.getLogger(this.getClass());
    private final String kibanaServerUsername;
    private final String kibanaIndexName;
    private final String kibanaIndexNamePrefix;
    private final Pattern versionedKibanaIndexPattern;
    private final boolean enabled;
    private final boolean privateTenantEnabled;
    private final ThreadContext threadContext;
    private final ImmutableList<String> indexSubNames = ImmutableList.of("alerting_cases", "analytics", "security_solution", "ingest");
    private final RoleBasedTenantAuthorization tenantAuthorization;
    private final FrontendDataMigrationInterceptor frontendDataMigrationInterceptor;
    private final RequestHandlerFactory requestHandlerFactory;
    private final TenantManager tenantManager;
    private final ClusterService clusterService;
    private final IndicesService indicesService;

    public MultiTenancyAuthorizationFilter(FeMultiTenancyConfig config, RoleBasedTenantAuthorization tenantAuthorization, TenantManager tenantManager,
                                           Actions actions, ThreadContext threadContext, Client nodeClient,
                                           RequestHandlerFactory requestHandlerFactory, ClusterService clusterService,
                                           IndicesService indicesService) {
        this.enabled = config.isEnabled();
        this.privateTenantEnabled = config.isPrivateTenantEnabled();
        this.kibanaServerUsername = config.getServerUsername();
        this.kibanaIndexName = config.getIndex();
        this.kibanaIndexNamePrefix = this.kibanaIndexName + "_";
        this.versionedKibanaIndexPattern = Pattern.compile(
                "(" + Pattern.quote(this.kibanaIndexName) + toPatternFragment(indexSubNames) + ")" + "(_[0-9]+\\.[0-9]+\\.[0-9]+(_[0-9]{3})?)?" + "(" + Pattern.quote(
                    TEMP_MIGRATION_INDEX_NAME_POSTFIX_1) + "|" + Pattern.quote(TEMP_MIGRATION_INDEX_NAME_POSTFIX_2) + ")?");

        this.KIBANA_ALL_SAVED_OBJECTS_WRITE = KibanaActionsProvider.getKibanaWriteAction(actions);
        this.KIBANA_ALL_SAVED_OBJECTS_READ = KibanaActionsProvider.getKibanaReadAction(actions);
        this.threadContext = threadContext;
        this.tenantAuthorization = tenantAuthorization;
        this.frontendDataMigrationInterceptor = new FrontendDataMigrationInterceptor(threadContext, nodeClient, config);
        this.requestHandlerFactory = requestHandlerFactory;
        this.tenantManager = tenantManager;
        this.clusterService = Objects.requireNonNull(clusterService, "Cluster services are required");
        this.indicesService = Objects.requireNonNull(indicesService, "Indices service is required");
        log.info("Filter which supports front-end multi tenancy created, enabled '{}'.", enabled);
    }

    @Override
    public SyncAuthorizationFilter.Result apply(PrivilegesEvaluationContext context, ActionListener<?> listener) {
        if (!enabled) {
            log.trace("MultiTenancyAuthorizationFilter is disabled");
            return SyncAuthorizationFilter.Result.OK;
        }

        if (threadContext.getHeader(SG_FILTER_LEVEL_FEMT_DONE) != null) {
            if (log.isDebugEnabled()) {
                log.debug("FEMT is already done for: " + threadContext.getHeader(SG_FILTER_LEVEL_FEMT_DONE));
            }

            return SyncAuthorizationFilter.Result.PASS_ON_FAST_LANE;
        }

        User user = context.getUser();
        ResolvedIndices requestedResolved = context.getRequestInfo().getResolvedIndices();

        if (log.isDebugEnabled()) {
            log.debug("apply(" + context.getAction() + ", " + user + ")\nrequestedResolved: " + requestedResolved + "\nrequestedTenant: "
                    + user.getRequestedTenant());
        }

        List<IndexInfo> kibanaIndicesInfo = checkForExclusivelyUsedKibanaIndexOrAlias((ActionRequest) context.getRequest(),
            requestedResolved, user.getRequestedTenant());

        if (kibanaIndicesInfo.isEmpty()) {
            // This is not about the .kibana index: Nothing to do here, get out early!
            return SyncAuthorizationFilter.Result.OK;
        }

        if (log.isDebugEnabled()) {
            log.debug("IndexInfo: " + kibanaIndicesInfo);
        }

        Result initializationProcessingResult = frontendDataMigrationInterceptor.process(getOriginalIndicesNames(kibanaIndicesInfo), context, listener);

        if (Result.PASS_ON_FAST_LANE != initializationProcessingResult) {
            return initializationProcessingResult;
        }

        String requestedTenant = user.getRequestedTenant();

        log.trace("User's '{}' requested tenant is '{}'", user.getName(), requestedTenant);

        if (tenantManager.isTenantHeaderEmpty(requestedTenant)) {
            //requestedTenant = Tenant.GLOBAL_TENANT_ID;
            return SyncAuthorizationFilter.Result.OK;
        }

        try {
            boolean frontendServerUser = user.getName().equals(kibanaServerUsername);
            TenantAccess tenantAccess = frontendServerUser ? TenantAccess.FULL_ACCESS : getTenantAccess(context, requestedTenant);

            Result result = handleRequestRequiringSpecialTreatment(tenantAccess, context, (ActionListener<BulkResponse>) listener);
            if(result == Result.INTERCEPTED) {
                return Result.INTERCEPTED;
            }

            if (isTenantAllowed(tenantAccess, context.getUser().getName(), context.getAction(), requestedTenant)) {
                return handle(context, listener);
            } else {
                return SyncAuthorizationFilter.Result.DENIED;
            }
        } catch (PrivilegesEvaluationException e) {
            log.error("Error while evaluating privileges for {}, tenant: {}", user, requestedTenant, e);
            return SyncAuthorizationFilter.Result.DENIED;
        }
    }

    /**
     * Method handle update request issued by search frontend when the dashboard is opened. User with read-only permission is not allowed
     * to perform the update. When SG returns 403 response then process of opening the dashboard is interrupted. Therefore, the method
     * replace response with code 403 to 404. Loading dashboard works correctly in case of 404 response (inside bulk request).
     */
    private SyncAuthorizationFilter.Result handleRequestRequiringSpecialTreatment(TenantAccess tenantAccess,
        PrivilegesEvaluationContext context, ActionListener<BulkResponse> listener) {
        boolean shouldReturnNotFound = tenantAccess.isReadOnly() &&
            "indices:data/write/bulk".equals(context.getAction().name()) &&
            (context.getRequest() instanceof BulkRequest) &&
            isUpdateRequestDuringLoadingDashboard((BulkRequest)context.getRequest());
        if(shouldReturnNotFound) {
            BulkRequest request = (BulkRequest) context.getRequest();
            notFoundBulkResponseForOnlyBulkRequest(listener, request);
            log.debug("Bulk only request permitted to load frontend dashboard.");
            return Result.INTERCEPTED;
        }
        return Result.PASS_ON_FAST_LANE;
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

    private SyncAuthorizationFilter.Result handle(PrivilegesEvaluationContext context, ActionListener<?> listener) {

        Object request = context.getRequest();
        Optional<RequestHandler<ActionRequest>> requestHandler = requestHandlerFactory.requestHandlerFor(request);

        String internalTenantName = tenantManager.toInternalTenantName(context.getUser());

        return requestHandler
                .map(handler -> handler.handle(context, internalTenantName, (ActionRequest) request, listener))
                .orElseGet(() -> {
                    if (log.isTraceEnabled()) {
                        log.trace("Not giving {} special treatment for FEMT", request);
                    }
                    return SyncAuthorizationFilter.Result.OK;
                });
    }

    private TenantAccess getTenantAccess(PrivilegesEvaluationContext context, String requestedTenant) throws PrivilegesEvaluationException {
        if (!tenantManager.isTenantHeaderValid(requestedTenant)) {
            log.warn("Invalid tenant: " + requestedTenant + "; user: " + context.getUser());

            return TenantAccess.INACCESSIBLE;
        }

        if (tenantManager.isUserTenantHeader(requestedTenant)) {
            return privateTenantEnabled? TenantAccess.FULL_ACCESS : TenantAccess.INACCESSIBLE;
        }

        return new TenantAccess(
            tenantAuthorization.hasTenantPermission(context, KIBANA_ALL_SAVED_OBJECTS_READ, requestedTenant).isOk(),
            tenantAuthorization.hasTenantPermission(context, KIBANA_ALL_SAVED_OBJECTS_WRITE, requestedTenant).isOk()
        );
    }

    private boolean isTenantAllowed(TenantAccess tenantAccess, String  username, Action action, String requestedTenant)
            throws PrivilegesEvaluationException {



        if (tenantAccess.isProhibited()) {
            log.warn("Tenant {} is not allowed for user {}", requestedTenant, username);
            return false;
        }

        if (tenantAccess.isWriteProhibited() && !READ_ONLY_ALLOWED_ACTIONS.contains(action.name())) {
            log.warn("Tenant {} is not allowed to write (user: {})", requestedTenant, username);
            return false;
        }

        return true;
    }

    private List<IndexInfo> checkForExclusivelyUsedKibanaIndexOrAlias(ActionRequest request, ResolvedIndices requestedResolved,
        String requestedTenant) {
        if (requestedResolved.isLocalAll()) {
            return Collections.emptyList();
        }
        Set<String> allQueryIndices = getIndices(request);
        List<IndexInfo> multiTenancyRelatedIndices = allQueryIndices//
            .stream() //
            .map(this::checkForExclusivelyUsedKibanaIndexOrAlias) //
            .filter(Objects::nonNull) //
            .collect(Collectors.toList());
        if ((!Strings.isNullOrEmpty(requestedTenant)) && (!multiTenancyRelatedIndices.isEmpty()) && (allQueryIndices.size() != multiTenancyRelatedIndices.size())) {
            String indicesNames = String.join(", ", allQueryIndices);
            log.error(
                "Request '{}' is related to multi-tenancy indices and some other indices '{}', requested tenant '{}'",
                request.getClass(),
                indicesNames,
                requestedTenant);
        }
        return multiTenancyRelatedIndices;
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
                return new IndexInfo(aliasOrIndex, matcher.group(1), matcher.group(2));
            }

        }

        return null;
    }

    private Set<String> getOriginalIndicesNames(List<IndexInfo> indicesInfo) {
        return indicesInfo.stream()
                .map(info -> info.originalName)
                .collect(Collectors.toSet());
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
                log.trace("Not supported for multi tenancy: {} ", request);
            }

            return Collections.emptySet();
        }
    }

    private class IndexInfo {
        final String originalName;
        final String prefix;
        final String suffix;

        IndexInfo(String originalName, String prefix, String suffix) {
            this.originalName = originalName;
            this.prefix = prefix;
            this.suffix = suffix;
        }

        @Override
        public String toString() {
            return "IndexInfo [originalName=" + originalName + ", prefix=" + prefix + ", suffix=" + suffix + "]";
        }

    }

    public boolean isEnabled() {
        return enabled;
    }

    private String toPatternFragment(Collection<String> indexSuffixes) {
        StringBuilder result = new StringBuilder("(?:");

        for (String suffix : indexSuffixes) {
            // An empty first element is expected in the result
            result.append("|_");
            result.append(Pattern.quote(suffix));
        }

        result.append(")");

        return result.toString();
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
}
