package com.floragunn.searchguard.enterprise.femt.request.mapper;

import com.floragunn.searchguard.enterprise.femt.RequestResponseTenantData;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.index.get.GetResult;

import java.util.Arrays;
import java.util.Optional;

public class MultiGetMapper extends RequestResponseMapper {

    public MultiGetRequest toScopedMultiGetRequest(MultiGetRequest request, String tenant) {
        log.debug("Rewriting multi get request - adding tenant scope");
        MultiGetRequest scopedRequest = new MultiGetRequest()
                .preference(request.preference())
                .realtime(request.realtime())
                .refresh(request.refresh());
        scopedRequest.setForceSyntheticSource(request.isForceSyntheticSource());

        request.getItems()
                .stream()
                .map(item -> addTenantScopeToMultiGetItem(item, tenant))
                .forEach(scopedRequest::add);
        return scopedRequest;
    }

    public MultiGetResponse toUnscopedMultiGetResponse(MultiGetResponse response) {
        log.debug("Rewriting multi get response - removing tenant scope");
        MultiGetItemResponse[] items = Arrays.stream(response.getResponses()) //
                .map(this::unscopeIdInMultiGetResponseItem)
                .toArray(MultiGetItemResponse[]::new);
        return new MultiGetResponse(items);
    }

    private MultiGetRequest.Item addTenantScopeToMultiGetItem(MultiGetRequest.Item item, String tenant) {
        if (log.isDebugEnabled()) {
            log.debug("Adding tenant scope to multi get item: {}, {}", item.index(), item.id());
        }
        return new MultiGetRequest.Item(item.index(), RequestResponseTenantData.scopedId(item.id(), tenant))
                .routing(item.routing())
                .storedFields(item.storedFields())
                .version(item.version())
                .versionType(item.versionType())
                .fetchSourceContext(item.fetchSourceContext());
    }

    private MultiGetItemResponse unscopeIdInMultiGetResponseItem(MultiGetItemResponse multiGetItemResponse) {
        log.debug("Removing tenant scope from multi get item response: {}, {}", multiGetItemResponse.getIndex(), multiGetItemResponse.getId());
        GetResponse successResponse = Optional.ofNullable(multiGetItemResponse.getResponse())
                .map(response -> new GetResult(response.getIndex(),
                        RequestResponseTenantData.unscopedId(response.getId()),
                        response.getSeqNo(),
                        response.getPrimaryTerm(),
                        response.getVersion(),
                        response.isExists(),
                        response.getSourceAsBytesRef(),
                        response.getFields(),
                        null))
                .map(GetResponse::new)
                .orElse(null);

        MultiGetResponse.Failure failure = Optional.ofNullable(multiGetItemResponse.getFailure()) //
                .map(fault -> new MultiGetResponse.Failure(fault.getIndex(), RequestResponseTenantData.unscopedId(fault.getId()), fault.getFailure())) //
                .orElse(null);
        return new MultiGetItemResponse(successResponse, failure);
    }

}
