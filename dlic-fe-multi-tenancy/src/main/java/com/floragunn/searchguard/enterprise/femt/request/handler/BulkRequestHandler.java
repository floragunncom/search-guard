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
import com.floragunn.searchguard.enterprise.femt.RequestResponseTenantData;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.seqno.SequenceNumbers;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static com.floragunn.searchguard.enterprise.femt.PrivilegesInterceptorImpl.SG_FILTER_LEVEL_FEMT_DONE;

public class BulkRequestHandler extends RequestHandler<BulkRequest> {

    private final Client nodeClient;
    private final ThreadContext threadContext;
    public BulkRequestHandler(Logger log, Client nodeClient, ThreadContext threadContext) {
        super(log);
        this.nodeClient = nodeClient;
        this.threadContext = threadContext;
    }

    @Override
    public SyncAuthorizationFilter.Result handle(PrivilegesEvaluationContext context, String requestedTenant, BulkRequest request, ActionListener<?> listener) {
        threadContext.putHeader(SG_FILTER_LEVEL_FEMT_DONE, request.toString());

        try (ThreadContext.StoredContext storedContext = threadContext.newStoredContext()) {
            for (DocWriteRequest<?> item : request.requests()) {
                String id = item.id();
                String newId;

                if (id == null) {
                    // TODO check uuid generation
                    // TODO - IndexRequest#autoGenerateId - uses UUIDs.base64UUID()
                    if (item instanceof IndexRequest) {
                        newId = RequestResponseTenantData.scopedId(UUID.randomUUID().toString(), requestedTenant);
                    } else {
                        newId = null;
                    }
                } else {
                    newId = RequestResponseTenantData.scopeIdIfNeeded(id, requestedTenant);
                }

                if (item instanceof IndexRequest indexRequest) {

                    indexRequest.id(newId);

                    Map<String, Object> source = indexRequest.sourceAsMap();

                    Map<String, Object> newSource = new LinkedHashMap<>(source);
                    RequestResponseTenantData.appendSgTenantFieldToSource(newSource, requestedTenant);

                    indexRequest.source(newSource, indexRequest.getContentType());
                } else if (item instanceof DeleteRequest) {
                    ((DeleteRequest) item).id(newId);
                } else if (item instanceof UpdateRequest) {
                    ((UpdateRequest) item).id(newId);
                } else {
                    log.error("Unhandled request {}", item);
                }
            }

            nodeClient.bulk(request, new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse response) {
                    log.debug("Process bulk response {}", response);
                    try {
                        storedContext.restore();

                        BulkItemResponse[] items = response.getItems();
                        BulkItemResponse[] newItems = new BulkItemResponse[items.length];

                        for (int i = 0; i < items.length; i++) {
                            BulkItemResponse item = items[i];

                            if (item.getFailure() != null) {
                                BulkItemResponse.Failure failure = item.getFailure();
                                BulkItemResponse.Failure interceptedFailure = failure;
                                // TODO is any simpler method to replace in failure?
                                //TODO I do not think so
                                if(failure.isAborted()) {
                                    interceptedFailure = new BulkItemResponse.Failure(failure.getIndex(), RequestResponseTenantData.unscopedId(failure.getId()), failure.getCause(), true);
                                } else if ((failure.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) || (failure.getTerm() != SequenceNumbers.UNASSIGNED_PRIMARY_TERM)) {
                                    interceptedFailure = new BulkItemResponse.Failure(failure.getIndex(), RequestResponseTenantData.unscopedId(failure.getId()), failure.getCause(), failure.getSeqNo(), failure.getTerm());
                                } else {
                                    interceptedFailure = new BulkItemResponse.Failure(failure.getIndex(), RequestResponseTenantData.unscopedId(failure.getId()), failure.getCause(), failure.getStatus());
                                }
                                newItems[i] = BulkItemResponse.failure(item.getItemId(), item.getOpType(), interceptedFailure);
                            } else {
                                DocWriteResponse docWriteResponse = item.getResponse();
                                DocWriteResponse newDocWriteResponse = null;

                                if (docWriteResponse instanceof IndexResponse) {
                                    log.debug("Rewriting index response");
                                    newDocWriteResponse = new IndexResponse(docWriteResponse.getShardId(), RequestResponseTenantData.unscopedId(docWriteResponse.getId()),
                                            docWriteResponse.getSeqNo(), docWriteResponse.getPrimaryTerm(), docWriteResponse.getVersion(),
                                            docWriteResponse.getResult() == DocWriteResponse.Result.CREATED);
                                } else if (docWriteResponse instanceof DeleteResponse) {
                                    log.debug("Rewriting delete response");
                                    newDocWriteResponse = new DeleteResponse(docWriteResponse.getShardId(),
                                            RequestResponseTenantData.unscopedId(docWriteResponse.getId()),
                                            docWriteResponse.getSeqNo(),
                                            docWriteResponse.getPrimaryTerm(),
                                            docWriteResponse.getVersion(),
                                            docWriteResponse.getResult() == DocWriteResponse.Result.DELETED);
                                } else if (docWriteResponse instanceof UpdateResponse) {
                                    newDocWriteResponse = handleUpdateResponse((UpdateResponse)docWriteResponse);
                                } else {
                                    log.debug("Bulk response '{}' will not be modified", docWriteResponse);
                                    newDocWriteResponse = docWriteResponse;
                                }
                                newDocWriteResponse.setShardInfo(docWriteResponse.getShardInfo());

                                newItems[i] = BulkItemResponse.success(item.getItemId(), item.getOpType(), newDocWriteResponse);
                            }
                        }

                        @SuppressWarnings("unchecked")
                        ActionListener<BulkResponse> bulkListener = (ActionListener<BulkResponse>) listener;
                        BulkResponse bulkResponse = new BulkResponse(newItems, response.getIngestTookInMillis());
                        bulkListener.onResponse(bulkResponse);
                        log.debug("Bulk request handled without errors");
                    } catch (Exception e) {
                        log.error("Error during handling bulk request response", e);
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

}
