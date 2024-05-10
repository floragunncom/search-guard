package com.floragunn.searchguard.enterprise.femt.request.mapper;

import com.floragunn.searchguard.enterprise.femt.RequestResponseTenantData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;

public class UpdateMapper implements Unscoper<UpdateResponse> {

    private final static Logger log = LogManager.getLogger(UpdateMapper.class);

    public UpdateRequest toScopedUpdateRequest(UpdateRequest request, String tenant) {
        log.debug("Rewriting update request - adding tenant scope");
        String newId = RequestResponseTenantData.scopeIdIfNeeded(request.id(), tenant);
        request.id(newId);
        return request;
    }

    @Override
    public UpdateResponse unscopeResponse(UpdateResponse response) {
        log.debug("Rewriting update response - removing tenant scope");
        UpdateResponse updateResponse = new UpdateResponse(
                response.getShardId(),
                RequestResponseTenantData.unscopedId(response.getId()),
                response.getSeqNo(),
                response.getPrimaryTerm(),
                response.getVersion(),
                response.getResult()
        );
        updateResponse.setForcedRefresh(response.forcedRefresh());
        updateResponse.setShardInfo(response.getShardInfo());
        updateResponse.setGetResult(response.getGetResult());
        updateResponse.setShardInfo(response.getShardInfo());
        return updateResponse;
    }

}
