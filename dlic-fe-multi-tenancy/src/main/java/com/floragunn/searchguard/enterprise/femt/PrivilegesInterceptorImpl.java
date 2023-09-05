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

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.documents.UnparsedDocument;
import com.floragunn.searchguard.enterprise.femt.request.handler.RequestHandler;
import com.floragunn.searchguard.enterprise.femt.request.handler.RequestHandlerFactory;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.rest.RestStatus;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.SyncAuthorizationFilter;
import com.floragunn.searchguard.authz.actions.Action;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector.ResolvedIndices;
import com.floragunn.searchguard.authz.actions.Actions;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;

public class PrivilegesInterceptorImpl implements SyncAuthorizationFilter {

    private static final String TEMP_MIGRATION_INDEX_NAME_POSTFIX = "_reindex_temp";

    private static final String USER_TENANT = "__user__";
    public static final String SG_FILTER_LEVEL_FEMT_DONE = ConfigConstants.SG_CONFIG_PREFIX + "filter_level_femt_done";

    private final Action KIBANA_ALL_SAVED_OBJECTS_WRITE;
    private final Action KIBANA_ALL_SAVED_OBJECTS_READ;

    protected final Logger log = LogManager.getLogger(this.getClass());
    private final String kibanaServerUsername;
    private final String kibanaIndexName;
    private final String kibanaIndexNamePrefix;
    private final Pattern versionedKibanaIndexPattern;
    private final ImmutableSet<String> tenantNames;
    private final boolean enabled;
    private final ThreadContext threadContext;
    private final Client nodeClient;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final ImmutableList<String> indexSubNames = ImmutableList.of("alerting_cases", "analytics", "security_solution", "ingest");
    private final RoleBasedTenantAuthorization tenantAuthorization;
    private final RequestHandlerFactory requestHandlerFactory;

    public PrivilegesInterceptorImpl(FeMultiTenancyConfig config, RoleBasedTenantAuthorization tenantAuthorization, ImmutableSet<String> tenantNames,
                                     Actions actions, ThreadContext threadContext, Client nodeClient, ClusterService clusterService, IndicesService indicesService) {
        this.enabled = config.isEnabled();
        this.kibanaServerUsername = config.getServerUsername();
        this.kibanaIndexName = config.getIndex();
        this.kibanaIndexNamePrefix = this.kibanaIndexName + "_";
        this.versionedKibanaIndexPattern = Pattern.compile(
                "(" + Pattern.quote(this.kibanaIndexName) + toPatternFragment(indexSubNames) + ")" + "(_[0-9]+\\.[0-9]+\\.[0-9]+(_[0-9]{3})?)?" + "(" + Pattern.quote(TEMP_MIGRATION_INDEX_NAME_POSTFIX) + ")?");

        this.tenantNames = tenantNames.with(Tenant.GLOBAL_TENANT_ID);
        this.KIBANA_ALL_SAVED_OBJECTS_WRITE = actions.get("kibana:saved_objects/_/write");
        this.KIBANA_ALL_SAVED_OBJECTS_READ = actions.get("kibana:saved_objects/_/read");
        this.threadContext = threadContext;
        this.nodeClient = nodeClient;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.tenantAuthorization = tenantAuthorization;
        this.requestHandlerFactory = new RequestHandlerFactory(this.nodeClient, this.threadContext, this.clusterService, this.indicesService);
        log.info("Filter which supports front-end multi tenancy created, enabled '{}'.", enabled);
    }

    @Override
    public SyncAuthorizationFilter.Result apply(PrivilegesEvaluationContext context, ActionListener<?> listener) {
        if (!enabled) {
            log.trace("PrivilegesInterceptorImpl is disabled");
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

        List<IndexInfo> kibanaIndicesInfo = checkForExclusivelyUsedKibanaIndexOrAlias((ActionRequest) context.getRequest(), requestedResolved);

        if (kibanaIndicesInfo.isEmpty()) {
            // This is not about the .kibana index: Nothing to do here, get out early!
            return SyncAuthorizationFilter.Result.OK;
        }

        if (log.isDebugEnabled()) {
            log.debug("IndexInfo: " + kibanaIndicesInfo);
        }

        // TODO check if user is allowed to perform the request
        if(isTempMigrationIndex(kibanaIndicesInfo) && (context.getRequest() instanceof BulkRequest)){
            log.debug("Temporary index '{}' used during migration detected.", kibanaIndicesInfo);
            return handleDataMigration(context, (BulkRequest) context.getRequest(), (ActionListener<BulkResponse>) listener);
        }

        // TODO check if user is allowed to perform the request
        if("indices:admin/mapping/put".equals(context.getAction().name())) {
            if(log.isDebugEnabled()) {
                String indicesNames = kibanaIndicesInfo.stream().map(indexInfo -> indexInfo.originalName).collect(Collectors.joining(", "));
                log.debug("Migration of mappings detected for index '{}'", indicesNames);
            }
            return extendIndexMappingWithMultiTenancyData((PutMappingRequest) context.getRequest(), (ActionListener<AcknowledgedResponse>)listener);
        }

        // TODO check if user is allowed to perform the request
        if("indices:admin/create".equals(context.getAction().name())) {
            return extendIndexMappingWithMultiTenancyData((CreateIndexRequest) context.getRequest(), (ActionListener<CreateIndexResponse>)listener);
        }

        String requestedTenant = user.getRequestedTenant();

        log.trace("User's '{}' requested tenant is '{}'", user.getName(), requestedTenant);

        if (requestedTenant == null || requestedTenant.length() == 0) {
            //requestedTenant = Tenant.GLOBAL_TENANT_ID;
            return SyncAuthorizationFilter.Result.OK;
        }
        try {
            if (user.getName().equals(kibanaServerUsername) || isTenantAllowed(context, (ActionRequest) context.getRequest(), context.getAction(), requestedTenant)) {
                return handle(context, requestedTenant, listener);
            } else {
                return SyncAuthorizationFilter.Result.DENIED;
            }
        } catch (PrivilegesEvaluationException e) {
            // TODO
            log.error(e, e);
            return SyncAuthorizationFilter.Result.DENIED;
        }
    }

    private Result handleDataMigration(PrivilegesEvaluationContext context, BulkRequest bulkRequest, ActionListener<BulkResponse> listener) {
        String actionName = context.getAction().name();
        log.info("Action '{}' invoked during index migration, request class '{}'.", actionName, context.getRequest().getClass());
        boolean requestExtended = false;
        for (DocWriteRequest<?> item : bulkRequest.requests()) {
            if(item instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) item;
                IndexInfo indexInfo = checkForExclusivelyUsedKibanaIndexOrAlias(indexRequest.index());
                if((indexInfo != null) && isTempMigrationIndex(Collections.singletonList(indexInfo))) {
                    Map<String, Object> source = indexRequest.sourceAsMap();
                    if(RequestResponseTenantData.isScopedId(indexRequest.id()) && (!RequestResponseTenantData.containsSgTenantField(source))) {
                        String tenantName = RequestResponseTenantData.extractTenantFromId(indexRequest.id());
                        log.info("Adding field '{}' to document '{}' from index '{}' with value '{}'.", RequestResponseTenantData.getSgTenantField(), indexRequest.id(), indexRequest.index(), tenantName);
                        RequestResponseTenantData.appendSgTenantFieldTo(source, tenantName);
                        indexRequest.source(source);
                        requestExtended = true;
                    } else {
                        log.info("Document '{}' contain {} field with value '{}'", indexRequest.id(), RequestResponseTenantData.getSgTenantField(), source.get(RequestResponseTenantData.getSgTenantField()));
                    }
                }
            }
        }
        if(requestExtended) {
            try (StoredContext ctx = threadContext.newStoredContext()) {
                threadContext.putHeader(SG_FILTER_LEVEL_FEMT_DONE, bulkRequest.toString());
                nodeClient.bulk(bulkRequest, listener);
                return Result.INTERCEPTED;
            }
        }
        return Result.OK;
    }

    private boolean isTempMigrationIndex(List<IndexInfo> indices) {
        return indices.stream()
            .map(info -> info.originalName)
            .filter(indexName -> indexName.endsWith(TEMP_MIGRATION_INDEX_NAME_POSTFIX))
            .count() > 0;
    }

    private Optional<DocNode> extendMappingsWithMultitenancy(String sourceMappings) throws DocumentParseException {
        UnparsedDocument<?> mappings = UnparsedDocument.from(sourceMappings, Format.JSON);
        DocNode node = mappings.parseAsDocNode();
        return extendMappingsWithMultitenancy(node);
    }

    private static Optional<DocNode> extendMappingsWithMultitenancy(DocNode node) {
        return Optional.of(node) //
            .filter(docNode -> docNode.hasNonNull("properties")) //
            .map(propertiesDocNode -> propertiesDocNode.getAsNode("properties")) //
            .filter(propertiesDocNode -> !RequestResponseTenantData.containsSgTenantField(propertiesDocNode)) //
            .map(propertiesDocNode -> propertiesDocNode.with(RequestResponseTenantData.getSgTenantField(), DocNode.of("type", "keyword"))) //
            .map(propertiesDocNode -> node.with("properties", propertiesDocNode));
    }

    private SyncAuthorizationFilter.Result extendIndexMappingWithMultiTenancyData(PutMappingRequest request,
        ActionListener<AcknowledgedResponse> listener) {
        String source = request.source();
        log.trace("Mappings for index '{}' will be extended to support multitenancy, current mappings '{}'", request.indices(), source);
        try (StoredContext ctx = threadContext.newStoredContext()){
            Optional<PutMappingRequest> newRequest =  extendMappingsWithMultitenancy(source)
                .map(docNode -> createExtendedPutMappingRequest(request, docNode));
            if(newRequest.isPresent()) {
                PutMappingRequest putMappingRequest = newRequest.get();
                threadContext.putHeader(SG_FILTER_LEVEL_FEMT_DONE, putMappingRequest.toString());
                nodeClient.admin().indices().putMapping(putMappingRequest, listener);
                return SyncAuthorizationFilter.Result.INTERCEPTED;
            } else {
                return Result.OK;
            }
        } catch (DocumentParseException e) {
            String message = "Cannot extend index mapping with information related to multi tenancy";
            listener.onFailure(new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR, e));
            return SyncAuthorizationFilter.Result.INTERCEPTED;
        }
    }

    private PutMappingRequest createExtendedPutMappingRequest(PutMappingRequest request, DocNode docNode) {
        log.debug("Update mapping request intercepted, new mappings '{}'", docNode.toJsonString());
        PutMappingRequest extendedRequest = new PutMappingRequest(request.indices())
            .source(docNode)
            .origin(request.origin())
            .writeIndexOnly(request.writeIndexOnly())
            .masterNodeTimeout(request.masterNodeTimeout())
            .timeout(request.timeout())
            .indicesOptions(request.indicesOptions());
        if(request.getConcreteIndex() != null) {
            extendedRequest.setConcreteIndex(request.getConcreteIndex());
        }
        return extendedRequest;
    }

    private Result extendIndexMappingWithMultiTenancyData(CreateIndexRequest request, ActionListener<CreateIndexResponse> listener) {
        String sourceMappings = request.mappings();
        if(Strings.isNullOrEmpty(sourceMappings)) {
            return Result.OK;
        }
        try (StoredContext ctx = threadContext.newStoredContext()){
            UnparsedDocument<?> mappings = UnparsedDocument.from(sourceMappings, Format.JSON);
            DocNode requestSource = mappings.parseAsDocNode();
            if(requestSource.hasNonNull("_doc")) {
                Optional<String> newMappings = extendMappingsWithMultitenancy(requestSource.getAsNode("_doc"))
                    .map(updatedMappings -> requestSource.with("_doc", updatedMappings))
                    .map(DocNode::toJsonString);
                if (newMappings.isPresent()) {
                    String extendedMappings = newMappings.get();
                    log.debug("Mappings extended during index creation '{}'", extendedMappings);
                    request.mapping(extendedMappings);
                    threadContext.putHeader(SG_FILTER_LEVEL_FEMT_DONE, request.toString());
                    nodeClient.admin().indices().create(request, listener);
                    return SyncAuthorizationFilter.Result.INTERCEPTED;
                }
            }
        } catch (DocumentParseException e) {
            String message = "Cannot extend index mapping with information related to multi tenancy during index creation";
            listener.onFailure(new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR, e));
            return SyncAuthorizationFilter.Result.INTERCEPTED;
        }
        //TODO return SyncAuthorizationFilter.Result.INTERCEPTED
        return Result.OK;
    }

    private SyncAuthorizationFilter.Result handle(PrivilegesEvaluationContext context, String requestedTenant, ActionListener<?> listener) {

        Object request = context.getRequest();

        RequestHandler<ActionRequest> requestHandler = requestHandlerFactory.requestHandlerFor(request);

        if (Objects.nonNull(requestHandler)) {
            return requestHandler.handle(context, requestedTenant, (ActionRequest) request, listener);
        }
        if (log.isTraceEnabled()) {
            log.trace("Not giving {} special treatment for FEMT", request);
        }
        return SyncAuthorizationFilter.Result.OK;
    }

    private boolean isTenantAllowed(PrivilegesEvaluationContext context, ActionRequest request, Action action, String requestedTenant)
            throws PrivilegesEvaluationException {

        if (!isTenantValid(requestedTenant)) {
            log.warn("Invalid tenant: " + requestedTenant + "; user: " + context.getUser());

            return false;
        }

        if (requestedTenant.equals(USER_TENANT)) {
            return true;
        }

        boolean hasReadPermission = tenantAuthorization.hasTenantPermission(context, KIBANA_ALL_SAVED_OBJECTS_READ, requestedTenant).isOk();
        boolean hasWritePermission = tenantAuthorization.hasTenantPermission(context, KIBANA_ALL_SAVED_OBJECTS_WRITE, requestedTenant).isOk();

        hasReadPermission |= hasWritePermission;

        if (!hasReadPermission) {
            log.warn("Tenant {} is not allowed for user {}", requestedTenant, context.getUser().getName());
            return false;
        }

        if (!hasWritePermission && action.name().startsWith("indices:data/write")) {
            log.warn("Tenant {} is not allowed to write (user: {})", requestedTenant, context.getUser().getName());
            return false;
        }

        return true;
    }

    private List<IndexInfo> checkForExclusivelyUsedKibanaIndexOrAlias(ActionRequest request, ResolvedIndices requestedResolved) {
        if (requestedResolved.isLocalAll()) {
            return Collections.emptyList();
        }
        Set<String> allQueryIndices = getIndices(request);
        List<IndexInfo> multiTenancyRelatedIndices = allQueryIndices//
            .stream() //
            .map(indexName -> checkForExclusivelyUsedKibanaIndexOrAlias(indexName)) //
            .filter(Objects::nonNull) //
            .collect(Collectors.toList());
        if((multiTenancyRelatedIndices.size() > 0) && (allQueryIndices.size() != multiTenancyRelatedIndices.size())) {
            // TODO this case is not handled correctly
            // TODO return empty list
            String indicesNames = String.join(", ", allQueryIndices);
            log.error("Request '{}' is related to multi-tenancy request and some other indices '{}'", request.getClass(), indicesNames);
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

    public String getKibanaIndex() {
        return kibanaIndexName;
    }

    public String getKibanaServerUser() {
        return kibanaServerUsername;
    }

    public Map<String, Boolean> mapTenants(User user, ImmutableSet<String> roles) {
        if (user == null) {
            return ImmutableMap.empty();
        }

        ImmutableMap.Builder<String, Boolean> result = new ImmutableMap.Builder<>(roles.size());
        result.put(user.getName(), true);

        PrivilegesEvaluationContext context = new PrivilegesEvaluationContext(user, roles, null, null, false, null, null);

        for (String tenant : this.tenantNames) {
            try {
                boolean hasReadPermission = tenantAuthorization.hasTenantPermission(context, KIBANA_ALL_SAVED_OBJECTS_READ, tenant).isOk();
                boolean hasWritePermission = tenantAuthorization.hasTenantPermission(context, KIBANA_ALL_SAVED_OBJECTS_WRITE, tenant).isOk();

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
}
