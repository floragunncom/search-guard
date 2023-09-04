/*
 * Copyright 2023 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.femt.request.handler;

import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.SyncAuthorizationFilter;
import com.floragunn.searchguard.enterprise.femt.request.RequestTenantData;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.floragunn.searchguard.enterprise.femt.PrivilegesInterceptorImpl.SG_FILTER_LEVEL_FEMT_DONE;

public class GetRequestHandler extends RequestHandler<GetRequest> {

    private final Client nodeClient;
    private final ThreadContext threadContext;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    public GetRequestHandler(Logger log, Client nodeClient, ThreadContext threadContext, ClusterService clusterService, IndicesService indicesService) {
        super(log);
        this.nodeClient = nodeClient;
        this.threadContext = threadContext;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
    }

    @Override
    public SyncAuthorizationFilter.Result handle(PrivilegesEvaluationContext context, String requestedTenant, GetRequest request, ActionListener<?> listener) {
        threadContext.putHeader(SG_FILTER_LEVEL_FEMT_DONE, request.toString());

        try (ThreadContext.StoredContext storedContext = threadContext.newStoredContext()) {
            SearchRequest searchRequest = new SearchRequest(request.indices());
            BoolQueryBuilder query = RequestTenantData.sgTenantIdsQuery(requestedTenant, request.id())
                    .must(RequestTenantData.sgTenantFieldQuery(requestedTenant));
            searchRequest.source(SearchSourceBuilder.searchSource().query(query).version(true).seqNoAndPrimaryTerm(true));

            nodeClient.search(searchRequest, new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse response) {
                    try {

                        storedContext.restore();

                        long hits = response.getHits().getTotalHits().value;

                        @SuppressWarnings("unchecked")
                        ActionListener<GetResponse> getListener = (ActionListener<GetResponse>) listener;
                        if (hits == 1) {
                            getListener.onResponse(new GetResponse(searchHitToGetResult(response.getHits().getAt(0))));
                        } else if (hits == 0) {
                            getListener.onResponse(new GetResponse(new GetResult(searchRequest.indices()[0], request.id(),
                                    SequenceNumbers.UNASSIGNED_SEQ_NO, SequenceNumbers.UNASSIGNED_PRIMARY_TERM, -1, false, null, null, null)));
                        } else {
                            log.error("Unexpected hit count " + hits + " in " + response);
                            listener.onFailure(new ElasticsearchSecurityException("Internal error during multi tenancy interception"));
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

        return new GetResult(hit.getIndex(), RequestTenantData.unscopedId(hit.getId()), hit.getSeqNo(), hit.getPrimaryTerm(), hit.getVersion(), true,
                hit.getSourceRef(), documentFields, metadataFields);
    }
}
