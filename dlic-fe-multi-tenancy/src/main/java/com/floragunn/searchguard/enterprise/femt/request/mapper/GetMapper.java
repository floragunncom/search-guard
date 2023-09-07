package com.floragunn.searchguard.enterprise.femt.request.mapper;

import com.floragunn.searchguard.enterprise.femt.RequestResponseTenantData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.index.get.GetResult;

public class GetMapper {

    private final static Logger log = LogManager.getLogger(GetMapper.class);

    public GetRequest toScopedGetRequest(GetRequest request, String tenant) {
        request.id(RequestResponseTenantData.scopedId(request.id(), tenant));
        return request;
    }

    public GetResponse toUnscopedGetResponse(GetResponse response) {
        log.debug("Rewriting get response - removing tenant scope");
        GetResult getResult = new GetResult(
                response.getIndex(),
                RequestResponseTenantData.unscopedId(response.getId()),
                response.getSeqNo(),
                response.getPrimaryTerm(),
                response.getVersion(),
                response.isExists(),
                response.getSourceAsBytesRef(),
                response.getFields(),
                null);
        return new GetResponse(getResult);
    }

}
