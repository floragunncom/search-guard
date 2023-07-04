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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

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

    private static final String USER_TENANT = "__user__";
    private static final String SG_FILTER_LEVEL_FEMT_DONE = ConfigConstants.SG_CONFIG_PREFIX + "filter_level_femt_done";

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
    private final ImmutableList<String> indexSubNames = ImmutableList.of("alerting_cases", "analytics", "security_solution", "ingest");
    private final RoleBasedTenantAuthorization tenantAuthorization;

    public PrivilegesInterceptorImpl(FeMultiTenancyConfig config, RoleBasedTenantAuthorization tenantAuthorization, ImmutableSet<String> tenantNames,
            Actions actions, ThreadContext threadContext, Client nodeClient) {
        this.enabled = config.isEnabled();
        this.kibanaServerUsername = config.getServerUsername();
        this.kibanaIndexName = config.getIndex();
        this.kibanaIndexNamePrefix = this.kibanaIndexName + "_";
        this.versionedKibanaIndexPattern = Pattern.compile(
                "(" + Pattern.quote(this.kibanaIndexName) + toPatternFragment(indexSubNames) + ")" + "(_[0-9]+\\.[0-9]+\\.[0-9]+(_[0-9]{3})?)?");

        this.tenantNames = tenantNames.with(Tenant.GLOBAL_TENANT_ID);
        this.KIBANA_ALL_SAVED_OBJECTS_WRITE = actions.get("kibana:saved_objects/_/write");
        this.KIBANA_ALL_SAVED_OBJECTS_READ = actions.get("kibana:saved_objects/_/read");
        this.threadContext = threadContext;
        this.nodeClient = nodeClient;
        this.tenantAuthorization = tenantAuthorization;
    }

    @Override
    public SyncAuthorizationFilter.Result apply(PrivilegesEvaluationContext context, ActionListener<?> listener) {
        if (!enabled) {
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

        if (user.getName().equals(kibanaServerUsername)) {
            return SyncAuthorizationFilter.Result.OK;
        }

        if (log.isDebugEnabled()) {
            log.debug("apply(" + context.getAction() + ", " + user + ")\nrequestedResolved: " + requestedResolved + "\nrequestedTenant: "
                    + user.getRequestedTenant());
        }

        IndexInfo kibanaIndexInfo = checkForExclusivelyUsedKibanaIndexOrAlias((ActionRequest) context.getRequest(), requestedResolved);

        if (kibanaIndexInfo == null) {
            // This is not about the .kibana index: Nothing to do here, get out early!
            return SyncAuthorizationFilter.Result.OK;
        }

        if (log.isDebugEnabled()) {
            log.debug("IndexInfo: " + kibanaIndexInfo);
        }

        String requestedTenant = user.getRequestedTenant();

        if (requestedTenant == null || requestedTenant.length() == 0) {
            //requestedTenant = Tenant.GLOBAL_TENANT_ID;
            return SyncAuthorizationFilter.Result.OK;
        }

        // TODO do we need auth token stuff here?

        try {
            if (isTenantAllowed(context, (ActionRequest) context.getRequest(), context.getAction(), requestedTenant)) {
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

    private SyncAuthorizationFilter.Result handle(PrivilegesEvaluationContext context, String requestedTenant, ActionListener<?> listener) {

        Object request = context.getRequest();

        if (request instanceof IndexRequest || request instanceof UpdateRequest || request instanceof DeleteRequest) {
            // These are converted into BulkRequests and handled then
            return SyncAuthorizationFilter.Result.OK;
        }

        try (StoredContext ctx = threadContext.newStoredContext()) {

            threadContext.putHeader(SG_FILTER_LEVEL_FEMT_DONE, request.toString());

            if (request instanceof SearchRequest) {
                return handle(context, requestedTenant, (SearchRequest) request, ctx, listener);
            } else if (request instanceof GetRequest) {
                return handle(context, requestedTenant, (GetRequest) request, ctx, listener);
            } else if (request instanceof MultiGetRequest) {
                return handle(context, requestedTenant, (MultiGetRequest) request, ctx, listener);
            } else if (request instanceof ClusterSearchShardsRequest) {
                return handle((ClusterSearchShardsRequest) request, ctx, listener);
            } else if (request instanceof BulkRequest) {
                return handle(context, requestedTenant, (BulkRequest) request, ctx, listener);
            } else {
                if (log.isTraceEnabled()) {
                    log.trace("Not giving {} special treatment for FEMT", request);
                }
                return SyncAuthorizationFilter.Result.OK;
            }
        }
    }

    private SyncAuthorizationFilter.Result handle(PrivilegesEvaluationContext context, String requestedTenant, SearchRequest searchRequest,
            StoredContext ctx, ActionListener<?> listener) {
        BoolQueryBuilder queryBuilder = createQueryExtension(requestedTenant, null);

        if (searchRequest.source().query() != null) {
            queryBuilder.must(searchRequest.source().query());
        }

        searchRequest.source().query(queryBuilder);

        nodeClient.search(searchRequest, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse response) {
                try {
                    ctx.restore();

                    @SuppressWarnings("unchecked")
                    ActionListener<SearchResponse> searchListener = (ActionListener<SearchResponse>) listener;

                    searchListener.onResponse(response);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });

        return SyncAuthorizationFilter.Result.INTERCEPTED;
    }

    private SyncAuthorizationFilter.Result handle(PrivilegesEvaluationContext context, String requestedTenant, GetRequest getRequest,
            StoredContext ctx, ActionListener<?> listener) {
        SearchRequest searchRequest = new SearchRequest(getRequest.indices());
        BoolQueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders.idsQuery().addIds(scopedId(getRequest.id(), requestedTenant)))
                .must(createQueryExtension(requestedTenant, null));
        searchRequest.source(SearchSourceBuilder.searchSource().query(query));

        nodeClient.search(searchRequest, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse response) {
                try {

                    ctx.restore();

                    long hits = response.getHits().getTotalHits().value;

                    @SuppressWarnings("unchecked")
                    ActionListener<GetResponse> getListener = (ActionListener<GetResponse>) listener;
                    if (hits == 1) {
                        getListener.onResponse(new GetResponse(searchHitToGetResult(response.getHits().getAt(0))));
                    } else if (hits == 0) {
                        getListener.onResponse(new GetResponse(new GetResult(searchRequest.indices()[0], getRequest.id(),
                                SequenceNumbers.UNASSIGNED_SEQ_NO, SequenceNumbers.UNASSIGNED_PRIMARY_TERM, -1, false, null, null, null)));
                    } else {
                        log.error("Unexpected hit count " + hits + " in " + response);
                        listener.onFailure(new ElasticsearchSecurityException("Internal error when performing DLS"));
                    }

                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });

        return SyncAuthorizationFilter.Result.INTERCEPTED;
    }

    private SyncAuthorizationFilter.Result handle(PrivilegesEvaluationContext context, String requestedTenant, MultiGetRequest multiGetRequest,
            StoredContext ctx, ActionListener<?> listener) {

        Map<String, Set<String>> idsGroupedByIndex = multiGetRequest.getItems().stream()
                .collect(Collectors.groupingBy((item) -> item.index(), Collectors.mapping((item) -> item.id(), Collectors.toSet())));
        Set<String> indices = idsGroupedByIndex.keySet();
        SearchRequest searchRequest = new SearchRequest(indices.toArray(new String[indices.size()]));

        BoolQueryBuilder query;

        if (indices.size() == 1) {
            Set<String> ids = idsGroupedByIndex.get(indices.iterator().next()).stream().map((id) -> scopedId(id, requestedTenant))
                    .collect(Collectors.toSet());
            query = QueryBuilders.boolQuery().must(QueryBuilders.idsQuery().addIds(ids.toArray(new String[ids.size()])))
                    .must(createQueryExtension(requestedTenant, null));
        } else {
            BoolQueryBuilder mgetQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);

            for (Map.Entry<String, Set<String>> entry : idsGroupedByIndex.entrySet()) {
                Set<String> ids = entry.getValue().stream().map((id) -> scopedId(id, requestedTenant)).collect(Collectors.toSet());

                BoolQueryBuilder indexQuery = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("_index", entry.getKey()))
                        .must(QueryBuilders.idsQuery().addIds(ids.toArray(new String[entry.getValue().size()])));

                mgetQuery.should(indexQuery);
            }

            query = QueryBuilders.boolQuery().must(mgetQuery).must(createQueryExtension(requestedTenant, null));
        }

        searchRequest.source(SearchSourceBuilder.searchSource().query(query));

        nodeClient.search(searchRequest, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse response) {
                try {

                    ctx.restore();

                    List<MultiGetItemResponse> itemResponses = new ArrayList<>(response.getHits().getHits().length);

                    for (SearchHit hit : response.getHits().getHits()) {
                        itemResponses.add(new MultiGetItemResponse(new GetResponse(searchHitToGetResult(hit)), null));
                    }

                    @SuppressWarnings("unchecked")
                    ActionListener<MultiGetResponse> multiGetListener = (ActionListener<MultiGetResponse>) listener;
                    multiGetListener.onResponse(new MultiGetResponse(itemResponses.toArray(new MultiGetItemResponse[itemResponses.size()])));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });

        return SyncAuthorizationFilter.Result.INTERCEPTED;
    }

    private SyncAuthorizationFilter.Result handle(PrivilegesEvaluationContext context, String requestedTenant, BulkRequest bulkRequest,
            StoredContext ctx, ActionListener<?> listener) {

        for (DocWriteRequest<?> item : bulkRequest.requests()) {
            String id = item.id();
            String newId;

            if (id == null) {
                // TODO check uuid generation
                if (item instanceof IndexRequest) {
                    newId = scopedId(UUID.randomUUID().toString(), requestedTenant);
                } else {
                    newId = null;
                }
            } else {
                newId = scopedId(id, requestedTenant);
            }

            if (item instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) item;

                indexRequest.id(newId);

                Map<String, Object> source = indexRequest.sourceAsMap();
                Object namespaces = source.get("namespaces");
                Collection<Object> newNamespaces;

                if (namespaces == null) {
                    newNamespaces = Arrays.asList("_sg_ten_" + requestedTenant);
                } else if (namespaces instanceof Collection) {
                    newNamespaces = new LinkedHashSet<Object>((Collection<?>) namespaces);
                    newNamespaces.add("_sg_ten_" + requestedTenant);
                } else {
                    newNamespaces = Arrays.asList(namespaces, "_sg_ten_" + requestedTenant);
                }

                Map<String, Object> newSource = new LinkedHashMap<>(source);
                newSource.put("namespaces", newNamespaces);

                indexRequest.source(newSource, indexRequest.getContentType());
            } else if (item instanceof DeleteRequest) {
                ((DeleteRequest) item).id(newId);
            } else if (item instanceof UpdateRequest) {
                ((UpdateRequest) item).id(newId);
            } else {
                log.error("Unhandled request {}", item);
            }
        }

        nodeClient.bulk(bulkRequest, new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse response) {
                try {
                    ctx.restore();

                    BulkItemResponse[] items = response.getItems();
                    BulkItemResponse[] newItems = new BulkItemResponse[items.length];

                    for (int i = 0; i < items.length; i++) {
                        BulkItemResponse item = items[i];

                        if (item.getFailure() != null) {
                            newItems[i] = BulkItemResponse.failure(item.getItemId(), item.getOpType(), item.getFailure());
                        } else {
                            DocWriteResponse docWriteResponse = item.getResponse();
                            DocWriteResponse newDocWriteResponse = null;

                            if (docWriteResponse instanceof IndexResponse) {
                                newDocWriteResponse = new IndexResponse(docWriteResponse.getShardId(), unscopedId(docWriteResponse.getId()),
                                        docWriteResponse.getSeqNo(), docWriteResponse.getPrimaryTerm(), docWriteResponse.getVersion(),
                                        docWriteResponse.getResult() == DocWriteResponse.Result.CREATED);
                            } else if (docWriteResponse instanceof DeleteResponse) {
                                newDocWriteResponse = new DeleteResponse(docWriteResponse.getShardId(), unscopedId(docWriteResponse.getId()),
                                        docWriteResponse.getSeqNo(), docWriteResponse.getPrimaryTerm(), docWriteResponse.getVersion(),
                                        docWriteResponse.getResult() == DocWriteResponse.Result.DELETED);
                            } else if (docWriteResponse instanceof UpdateResponse) {
                                newDocWriteResponse = new UpdateResponse(docWriteResponse.getShardId(), unscopedId(docWriteResponse.getId()),
                                        docWriteResponse.getSeqNo(), docWriteResponse.getPrimaryTerm(), docWriteResponse.getVersion(),
                                        docWriteResponse.getResult());
                            } else {
                                newDocWriteResponse = docWriteResponse;
                            }

                            newItems[i] = BulkItemResponse.success(item.getItemId(), item.getOpType(), newDocWriteResponse);
                        }
                    }

                    @SuppressWarnings("unchecked")
                    ActionListener<BulkResponse> bulkListener = (ActionListener<BulkResponse>) listener;
                    bulkListener.onResponse(new BulkResponse(newItems, response.getIngestTookInMillis()));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });

        return SyncAuthorizationFilter.Result.INTERCEPTED;
    }

    private SyncAuthorizationFilter.Result handle(ClusterSearchShardsRequest request, StoredContext ctx, ActionListener<?> listener) {
        listener.onFailure(new ElasticsearchSecurityException(
                "Filter-level MT via cross cluster search is not available for scrolling and minimize_roundtrips=true"));
        return SyncAuthorizationFilter.Result.INTERCEPTED;
    }

    private String scopedId(String id, String tenant) {
        return id + "__sg_ten__" + tenant;
    }

    private String unscopedId(String id) {
        int i = id.indexOf("__sg_ten__");

        if (i != -1) {
            return id.substring(0, i);
        } else {
            return id;
        }
    }

    private GetResult searchHitToGetResult(SearchHit hit) {

        if (log.isDebugEnabled()) {
            log.debug("Converting to GetResult:\n" + hit);
        }

        Map<String, DocumentField> fields = hit.getFields();
        Map<String, DocumentField> documentFields;
        Map<String, DocumentField> metadataFields;

        if (fields.isEmpty()) {
            documentFields = Collections.emptyMap();
            metadataFields = Collections.emptyMap();
        } else {
            // TODO
            //            IndexMetadata indexMetadata = clusterService.state().getMetadata().indices().get(hit.getIndex());
            //            IndexService indexService = indexMetadata != null ? indicesService.indexService(indexMetadata.getIndex()) : null;
            //
            //            if (indexService != null) {
            //                documentFields = new HashMap<>(fields.size());
            //                metadataFields = new HashMap<>();
            //                MapperService mapperService = indexService.mapperService();
            //
            //                for (Map.Entry<String, DocumentField> entry : fields.entrySet()) {
            //                    if (mapperService.isMetadataField(entry.getKey())) {
            //                        metadataFields.put(entry.getKey(), entry.getValue());
            //                    } else {
            //                        documentFields.put(entry.getKey(), entry.getValue());
            //                    }
            //                }
            //
            //                if (log.isDebugEnabled()) {
            //                    log.debug("Partitioned fields: " + metadataFields + "; " + documentFields);
            //                }
            //
            //            } else {
            //                if (log.isWarnEnabled()) {
            //                    log.warn("Could not find IndexService for " + hit.getIndex() + "; assuming all fields as document fields."
            //                            + "This should not happen, however this should also not pose a big problem as ES mixes the fields again anyway.\n"
            //                            + "IndexMetadata: " + indexMetadata);
            //                }

            documentFields = fields;
            metadataFields = Collections.emptyMap();
            //   }
        }

        return new GetResult(hit.getIndex(), unscopedId(hit.getId()), hit.getSeqNo(), hit.getPrimaryTerm(), hit.getVersion(), true,
                hit.getSourceRef(), documentFields, metadataFields);
    }

    private BoolQueryBuilder createQueryExtension(String requestedTenant, String localClusterAlias) {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().minimumShouldMatch(1);

        // TODO better tenant id
        queryBuilder.should(QueryBuilders.termQuery("namespaces", "_sg_ten_" + requestedTenant));

        return queryBuilder;
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
