/*
 * Copyright 2023-2024 by floragunn GmbH - All rights reserved
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
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;

public class DeleteMapper implements Unscoper<DeleteResponse> {

    private final static Logger log = LogManager.getLogger(DeleteMapper.class);

    @Override
    public DeleteResponse unscopeResponse(DeleteResponse response) {
        log.debug("Rewriting delete response - removing tenant scope");
        DeleteResponse deleteResponse = new DeleteResponse(
                response.getShardId(), RequestResponseTenantData.unscopedId(response.getId()),
                response.getSeqNo(), response.getPrimaryTerm(),
                response.getVersion(), response.getResult() == DocWriteResponse.Result.DELETED
        );
        deleteResponse.setShardInfo(response.getShardInfo());
        return deleteResponse;
    }
    
}
