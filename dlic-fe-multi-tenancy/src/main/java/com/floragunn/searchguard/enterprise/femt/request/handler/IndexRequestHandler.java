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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;

public class IndexRequestHandler extends RequestHandler<IndexRequest> {

    @Override
    public SyncAuthorizationFilter.Result handle(PrivilegesEvaluationContext context, String requestedTenant, IndexRequest request, ActionListener<?> listener) {
        // This is converted into BulkRequests and handled then
        log.debug("Handle index request, return PASS_ON_FAST_LANE");
        return SyncAuthorizationFilter.Result.PASS_ON_FAST_LANE;
    }

}
