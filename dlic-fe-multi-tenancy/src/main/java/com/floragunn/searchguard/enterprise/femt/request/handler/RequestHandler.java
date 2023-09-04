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
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.update.UpdateResponse;

public abstract class RequestHandler<T extends ActionRequest> {

    protected final Logger log;

    protected RequestHandler(Logger log) {
        this.log = log;
    }

    public abstract SyncAuthorizationFilter.Result handle(PrivilegesEvaluationContext context, String requestedTenant, T request, ActionListener<?> listener);

    protected UpdateResponse handleUpdateResponse(UpdateResponse docWriteResponse) {
        log.debug("Rewriting update response");
        UpdateResponse updateResponse = new UpdateResponse(
                docWriteResponse.getShardId(),
                RequestResponseTenantData.unscopedId(docWriteResponse.getId()),
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
