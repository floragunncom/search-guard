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

package com.floragunn.searchguard.enterprise.femt.request.mapper;

import com.floragunn.searchguard.enterprise.femt.RequestResponseTenantData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.seqno.SequenceNumbers;

import java.util.LinkedHashMap;
import java.util.Map;

public class BulkMapper implements Unscoper<BulkResponse> {

    private final static Logger log = LogManager.getLogger(BulkMapper.class);

    public BulkRequest toScopedBulkRequest(BulkRequest request, String tenant) {
        log.debug("Rewriting bulk request - adding tenant scope");
        for (DocWriteRequest<?> item : request.requests()) {
            String id = item.id();
            String newId;

            if (id == null) {
                if (item instanceof IndexRequest) {
                    newId = RequestResponseTenantData.scopedId(UUIDs.base64UUID(), tenant);
                } else {
                    newId = null;
                }
            } else {
                newId = RequestResponseTenantData.scopeIdIfNeeded(id, tenant);
            }

            if (item instanceof IndexRequest indexRequest) {
                log.debug("Rewriting item - index request");
                indexRequest.id(newId);

                Map<String, Object> source = XContentHelper.convertToMap(indexRequest.source(), true, indexRequest.getContentType()).v2();

                Map<String, Object> newSource = new LinkedHashMap<>(source);
                RequestResponseTenantData.appendSgTenantFieldTo(newSource, tenant);

                indexRequest.source(newSource, indexRequest.getContentType());
            } else if (item instanceof DeleteRequest) {
                log.debug("Rewriting item - delete request");
                ((DeleteRequest) item).id(newId);
            } else if (item instanceof UpdateRequest) {
                log.debug("Rewriting item - update request");
                ((UpdateRequest) item).id(newId);
            } else {
                log.error("Rewriting item - unhandled item type {}", item.getClass().getName());
            }
        }
        return request;
    }

    @Override
    public BulkResponse unscopeResponse(BulkResponse response) {
        log.debug("Rewriting bulk response - removing tenant scope");
        BulkItemResponse[] items = response.getItems();
        BulkItemResponse[] newItems = new BulkItemResponse[items.length];

        for (int i = 0; i < items.length; i++) {
            BulkItemResponse item = items[i];

            if (item.getFailure() != null) {
                BulkItemResponse.Failure interceptedFailure = toUnscopedFailure(item.getFailure());
                newItems[i] = BulkItemResponse.failure(item.getItemId(), item.getOpType(), interceptedFailure);
            } else {
                DocWriteResponse docWriteResponse = item.getResponse();
                DocWriteResponse newDocWriteResponse = docWriteResponse;

                if (docWriteResponse instanceof IndexResponse indexResponse) {
                    log.debug("Rewriting item - index response");
                    newDocWriteResponse = new IndexMapper().unscopeResponse(indexResponse);
                } else if (docWriteResponse instanceof DeleteResponse deleteResponse) {
                    log.debug("Rewriting item - delete response");
                    newDocWriteResponse = new DeleteMapper().unscopeResponse(deleteResponse);
                } else if (docWriteResponse instanceof UpdateResponse updateResponse) {
                    log.debug("Rewriting item - update response");
                    newDocWriteResponse = new UpdateMapper().unscopeResponse(updateResponse);
                } else {
                    log.error("Rewriting item - unhandled item type {}", item.getClass().getName());
                }
                newDocWriteResponse.setForcedRefresh(docWriteResponse.forcedRefresh());
                newItems[i] = BulkItemResponse.success(item.getItemId(), item.getOpType(), newDocWriteResponse);
            }
        }
        return new BulkResponse(newItems, response.getTook().millis());
    }

    BulkItemResponse.Failure toUnscopedFailure(BulkItemResponse.Failure scoped) {
        log.debug("Rewriting item with failure");
        if (scoped.isAborted()) {
            return new BulkItemResponse.Failure(
                scoped.getIndex(),
                RequestResponseTenantData.unscopedId(scoped.getId()),
                scoped.getCause(),
                true);
        } else if ((scoped.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) || (scoped.getTerm() != SequenceNumbers.UNASSIGNED_PRIMARY_TERM)) {
            return new BulkItemResponse.Failure(
                scoped.getIndex(),
                RequestResponseTenantData.unscopedId(scoped.getId()),
                scoped.getCause(),
                scoped.getSeqNo(),
                scoped.getTerm());
        } else {
            return new BulkItemResponse.Failure(
                scoped.getIndex(),
                RequestResponseTenantData.unscopedId(scoped.getId()),
                scoped.getCause(),
                scoped.getStatus());
        }
    }

}
