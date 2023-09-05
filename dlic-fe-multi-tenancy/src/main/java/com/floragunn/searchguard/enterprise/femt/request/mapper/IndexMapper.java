package com.floragunn.searchguard.enterprise.femt.request.mapper;

import com.floragunn.searchguard.enterprise.femt.RequestResponseTenantData;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexResponse;

public class IndexMapper extends RequestResponseMapper {
    
    public IndexResponse toUnscopedIndexResponse(IndexResponse response) {
        log.debug("Rewriting index response - removing tenant scope");
        IndexResponse indexResponse = new IndexResponse(
                response.getShardId(), RequestResponseTenantData.unscopedId(response.getId()),
                response.getSeqNo(), response.getPrimaryTerm(), response.getVersion(),
                response.getResult() == DocWriteResponse.Result.CREATED
        );
        indexResponse.setShardInfo(response.getShardInfo());
        return indexResponse;
    }
    
}
