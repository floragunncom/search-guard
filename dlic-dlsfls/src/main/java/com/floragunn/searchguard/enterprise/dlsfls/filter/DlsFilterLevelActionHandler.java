/*
 * Copyright 2021-2022 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.dlsfls.filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShardsRequest;
import org.elasticsearch.action.search.TransportSearchScrollAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.floragunn.searchguard.authz.DocumentWhitelist;
import com.floragunn.searchguard.authz.SyncAuthorizationFilter;
import com.floragunn.searchguard.authz.actions.Action;
import com.floragunn.searchguard.authz.actions.ResolvedIndices;
import com.floragunn.searchguard.enterprise.dlsfls.DlsRestriction;
import com.floragunn.searchguard.queries.QueryBuilderTraverser;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchsupport.meta.Meta;
import com.floragunn.searchsupport.util.LocalClusterAliasExtractor;

public class DlsFilterLevelActionHandler {

    private static final Logger log = LogManager.getLogger(DlsFilterLevelActionHandler.class);

    public static SyncAuthorizationFilter.Result handle(Action action, ActionRequest request, ActionListener<?> listener,
            DlsRestriction.IndexMap restrictionMap, ResolvedIndices resolved, Client nodeClient, ClusterService clusterService,
            IndicesService indicesService, IndexNameExpressionResolver resolver, ThreadContext threadContext) {

        if (threadContext.getHeader(ConfigConstants.SG_FILTER_LEVEL_DLS_DONE) != null) {
            return SyncAuthorizationFilter.Result.OK;
        }

        String actionName = action.name();

        if (actionName.startsWith("searchguard:cluster:") || actionName.startsWith("cluster:") || actionName.startsWith("indices:admin/template/")
                || actionName.startsWith("indices:admin/index_template/")) {
            return SyncAuthorizationFilter.Result.OK;
        }

        if (actionName.startsWith(TransportSearchScrollAction.TYPE.name())) {
            return SyncAuthorizationFilter.Result.OK;
        }

        if (actionName.equals("indices:data/read/search/template") || actionName.equals("indices:data/read/msearch/template")) {
            // Let it pass; DLS will be handled on a lower level
            return SyncAuthorizationFilter.Result.OK;
        }

        if (request instanceof MultiSearchRequest) {
            // Let it pass; DLS will be handled on a lower level
            return SyncAuthorizationFilter.Result.OK;
        }

        return new DlsFilterLevelActionHandler(action.name(), request, listener, restrictionMap, resolved, nodeClient, clusterService, indicesService,
                resolver, threadContext).handle();
    }

    private final String action;
    private final ActionRequest request;
    private final ActionListener<?> listener;
    private final DlsRestriction.IndexMap restrictionMap;
    private final ResolvedIndices resolved;
    private final boolean requiresIndexScoping;
    private final Client nodeClient;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final ThreadContext threadContext;

    private BoolQueryBuilder filterLevelQueryBuilder;
    private DocumentWhitelist documentWhitelist;

    DlsFilterLevelActionHandler(String action, ActionRequest request, ActionListener<?> listener, DlsRestriction.IndexMap restrictionMap,
            ResolvedIndices resolved, Client nodeClient, ClusterService clusterService, IndicesService indicesService,
            IndexNameExpressionResolver resolver, ThreadContext threadContext) {
        this.action = action;
        this.listener = listener;
        this.request = request;
        this.restrictionMap = restrictionMap;
        this.resolved = resolved;
        this.nodeClient = nodeClient;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.threadContext = threadContext;

        this.requiresIndexScoping = resolved.isLocalAll() || resolved.getLocalAndRemoteIndices().size() != 1;
    }

    private SyncAuthorizationFilter.Result handle() {

        try (StoredContext ctx = threadContext.newStoredContext()) {

            threadContext.putHeader(ConfigConstants.SG_FILTER_LEVEL_DLS_DONE, request.toString());

            try {
                if (!createQueryExtension()) {
                    return SyncAuthorizationFilter.Result.OK;
                }

                if (log.isDebugEnabled()) {
                    log.debug("Created filterLevelQuery for " + request + ":\n" + filterLevelQueryBuilder);
                }

            } catch (Exception e) {
                log.error("Unable to handle filter level DLS", e);
                throw new RuntimeException(e);
            }

            if (filterLevelQueryBuilder == null) {
                return SyncAuthorizationFilter.Result.OK;
            }

            if (request instanceof SearchRequest) {
                return handle((SearchRequest) request, ctx);
            } else if (request instanceof GetRequest) {
                return handle((GetRequest) request, ctx);
            } else if (request instanceof MultiGetRequest) {
                return handle((MultiGetRequest) request, ctx);
            } else if (request instanceof ClusterSearchShardsRequest) {
                return handle((ClusterSearchShardsRequest) request, ctx);
            } else if (request instanceof SearchShardsRequest) {
                return handle((SearchShardsRequest) request, ctx);
            } else {
                log.error("Unsupported request type for filter level DLS: " + request);
                throw new RuntimeException("Unsupported request type for filter level DLS: " + action + "; " + request.getClass().getName());
            }
        }
    }

    private SyncAuthorizationFilter.Result handle(SearchRequest searchRequest, StoredContext ctx) {
        if (documentWhitelist != null) {
            documentWhitelist.applyTo(threadContext);
        }

        String localClusterAlias = LocalClusterAliasExtractor.getLocalClusterAliasFromSearchRequest(searchRequest);

        if (localClusterAlias != null) {
            try {
                createQueryExtension(localClusterAlias);
            } catch (Exception e) {
                log.error("Unable to handle filter level DLS", e);
                throw new ElasticsearchSecurityException("Unable to handle filter level DLS", e);
            }
        }

        if (searchRequest.source().query() != null) {
            filterLevelQueryBuilder.must(searchRequest.source().query());
        }

        searchRequest.source().query(filterLevelQueryBuilder);

        if (log.isTraceEnabled()) {
            log.trace("Executing search request {}", searchRequest);
        }

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
                log.error("DLS filter level failure", e);
                listener.onFailure(e);
            }
        });

        return SyncAuthorizationFilter.Result.INTERCEPTED;
    }

    private SyncAuthorizationFilter.Result handle(GetRequest getRequest, StoredContext ctx) {
        if (documentWhitelist != null) {
            documentWhitelist.applyTo(threadContext);
        }

        SearchRequest searchRequest = new SearchRequest(getRequest.indices());
        BoolQueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders.idsQuery().addIds(getRequest.id())).must(filterLevelQueryBuilder);
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

    private SyncAuthorizationFilter.Result handle(MultiGetRequest multiGetRequest, StoredContext ctx) {
        if (documentWhitelist != null) {
            documentWhitelist.applyTo(threadContext);
        }

        Map<String, Set<String>> idsGroupedByIndex = multiGetRequest.getItems().stream()
                .collect(Collectors.groupingBy((item) -> item.index(), Collectors.mapping((item) -> item.id(), Collectors.toSet())));
        Set<String> indices = idsGroupedByIndex.keySet();
        SearchRequest searchRequest = new SearchRequest(indices.toArray(new String[indices.size()]));

        BoolQueryBuilder query;

        if (indices.size() == 1) {
            Set<String> ids = idsGroupedByIndex.get(indices.iterator().next());
            query = QueryBuilders.boolQuery().must(QueryBuilders.idsQuery().addIds(ids.toArray(new String[ids.size()])))
                    .must(filterLevelQueryBuilder);
        } else {
            BoolQueryBuilder mgetQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);

            for (Map.Entry<String, Set<String>> entry : idsGroupedByIndex.entrySet()) {
                BoolQueryBuilder indexQuery = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("_index", entry.getKey()))
                        .must(QueryBuilders.idsQuery().addIds(entry.getValue().toArray(new String[entry.getValue().size()])));

                mgetQuery.should(indexQuery);
            }

            query = QueryBuilders.boolQuery().must(mgetQuery).must(filterLevelQueryBuilder);
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

    private SyncAuthorizationFilter.Result handle(ClusterSearchShardsRequest request, StoredContext ctx) {
        listener.onFailure(new ElasticsearchSecurityException(
                "Filter-level DLS via cross cluster search is not available for scrolling and minimize_roundtrips=true"));
        return SyncAuthorizationFilter.Result.INTERCEPTED;
    }

    private SyncAuthorizationFilter.Result handle(SearchShardsRequest request, StoredContext ctx) {
        listener.onFailure(new ElasticsearchSecurityException(
                "Filter-level DLS via cross cluster search is not available for scrolling and minimize_roundtrips=true"));
        return SyncAuthorizationFilter.Result.INTERCEPTED;
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
            IndexMetadata indexMetadata = clusterService.state().getMetadata().indices().get(hit.getIndex());
            IndexService indexService = indexMetadata != null ? indicesService.indexService(indexMetadata.getIndex()) : null;

            if (indexService != null) {
                documentFields = new HashMap<>(fields.size());
                metadataFields = new HashMap<>();
                MapperService mapperService = indexService.mapperService();

                for (Map.Entry<String, DocumentField> entry : fields.entrySet()) {
                    if (mapperService.isMetadataField(entry.getKey())) {
                        metadataFields.put(entry.getKey(), entry.getValue());
                    } else {
                        documentFields.put(entry.getKey(), entry.getValue());
                    }
                }

                if (log.isDebugEnabled()) {
                    log.debug("Partitioned fields: " + metadataFields + "; " + documentFields);
                }

            } else {
                if (log.isWarnEnabled()) {
                    log.warn("Could not find IndexService for " + hit.getIndex() + "; assuming all fields as document fields."
                            + "This should not happen, however this should also not pose a big problem as ES mixes the fields again anyway.\n"
                            + "IndexMetadata: " + indexMetadata);
                }

                documentFields = fields;
                metadataFields = Collections.emptyMap();
            }
        }

        return new GetResult(hit.getIndex(), hit.getId(), hit.getSeqNo(), hit.getPrimaryTerm(), hit.getVersion(), true, hit.getSourceRef(),
                documentFields, metadataFields);
    }

    private boolean createQueryExtension() throws IOException {
        return createQueryExtension(null);
    }

    private boolean createQueryExtension(String localClusterAlias) throws IOException {
        BoolQueryBuilder dlsQueryBuilder = QueryBuilders.boolQuery().minimumShouldMatch(1);
        DocumentWhitelist.Builder documentWhitelist = new DocumentWhitelist.Builder();

        int queryCount = 0;

        for (Meta.IndexLikeObject index : Meta.IndexLikeObject.resolveDeep(resolved.getLocal().getUnion())) {
            String prefixedIndex;

            if (localClusterAlias != null) {
                prefixedIndex = localClusterAlias + ":" + index.name();
            } else {
                prefixedIndex = index.name();
            }

            DlsRestriction dlsRestriction = this.restrictionMap.getIndexMap().get(index);

            if (dlsRestriction == null || dlsRestriction.isUnrestricted()) {
                if (requiresIndexScoping) {
                    // This index has no DLS configured, thus it is unrestricted.
                    // To allow the index in a complex query, we need to add the query below to let the index pass.
                    dlsQueryBuilder.should(QueryBuilders.termQuery("_index", prefixedIndex));
                }
                continue;
            }

            for (com.floragunn.searchsupport.queries.Query dlsQuery : dlsRestriction.getQueries()) {
                queryCount++;

                QueryBuilder parsedDlsQuery = dlsQuery.getQueryBuilder();

                if (!requiresIndexScoping) {
                    dlsQueryBuilder.should(parsedDlsQuery);
                } else {
                    // The original request referred to several indices. That's why we have to scope each query to the index it is meant for
                    dlsQueryBuilder.should(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("_index", prefixedIndex)).must(parsedDlsQuery));
                }

                Set<QueryBuilder> queryBuilders = QueryBuilderTraverser.findAll(parsedDlsQuery,
                        (q) -> (q instanceof TermsQueryBuilder) && ((TermsQueryBuilder) q).termsLookup() != null);

                for (QueryBuilder queryBuilder : queryBuilders) {
                    TermsQueryBuilder termsQueryBuilder = (TermsQueryBuilder) queryBuilder;

                    documentWhitelist.add(termsQueryBuilder.termsLookup().index(), termsQueryBuilder.termsLookup().id());
                }
            }
        }

        if (requiresIndexScoping) {
            // We also need to add remote indices to the query to let them pass

            for (String remoteIndex : resolved.getRemoteIndices()) {
                String prefixedIndex;

                if (localClusterAlias != null) {
                    prefixedIndex = localClusterAlias + ":" + remoteIndex;
                } else {
                    prefixedIndex = remoteIndex;
                }

                // This index has no DLS configured, thus it is unrestricted.
                // To allow the index in a complex query, we need to add the query below to let the index pass.
                dlsQueryBuilder.should(QueryBuilders.termQuery("_index", prefixedIndex));

            }
        }

        if (queryCount == 0) {
            // Return false to indicate that no query manipulation is necessary
            return false;
        } else {
            this.filterLevelQueryBuilder = dlsQueryBuilder;
            this.documentWhitelist = documentWhitelist.build();
            return true;
        }
    }

}
