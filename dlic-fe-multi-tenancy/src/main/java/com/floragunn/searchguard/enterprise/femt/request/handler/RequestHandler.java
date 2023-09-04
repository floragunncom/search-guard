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
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

public abstract class RequestHandler<T extends ActionRequest> {

    //todo move to separate class?
    protected static final String SG_TENANT_FIELD = "sg_tenant";
    private static final String TENAND_SEPARATOR_IN_ID = "__sg_ten__";

    protected final Logger log;

    protected RequestHandler(Logger log) {
        this.log = log;
    }

    public abstract SyncAuthorizationFilter.Result handle(PrivilegesEvaluationContext context, String requestedTenant, T request, ActionListener<?> listener);

    protected BoolQueryBuilder createQueryExtension(String requestedTenant, String localClusterAlias) {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().minimumShouldMatch(1);

        // TODO better tenant id
        queryBuilder.should(QueryBuilders.termQuery(SG_TENANT_FIELD, requestedTenant));

        return queryBuilder;
    }

    protected String unscopedId(String id) {
        int i = id.indexOf(TENAND_SEPARATOR_IN_ID);

        if (i != -1) {
            return id.substring(0, i);
        } else {
            return id;
        }
    }

    protected String scopedId(String id, String tenant) {
        return id + TENAND_SEPARATOR_IN_ID + tenant;
    }

    protected String scopeIdIfNeeded(String id, String tenant) {
        if(id.contains(TENAND_SEPARATOR_IN_ID)) {
            return scopedId(unscopedId(id), tenant);
        }
        return scopedId(id, tenant);
    }

    protected UpdateResponse handleUpdateResponse(UpdateResponse docWriteResponse) {
        log.debug("Rewriting update response");
        UpdateResponse updateResponse = new UpdateResponse(
                docWriteResponse.getShardId(),
                unscopedId(docWriteResponse.getId()),
                docWriteResponse.getSeqNo(),
                docWriteResponse.getPrimaryTerm(),
                docWriteResponse.getVersion(),
                docWriteResponse.getResult());
        updateResponse.setForcedRefresh(docWriteResponse.forcedRefresh());
        updateResponse.setShardInfo(docWriteResponse.getShardInfo());
        updateResponse.setGetResult(docWriteResponse.getGetResult());
        return updateResponse;
    }

}
