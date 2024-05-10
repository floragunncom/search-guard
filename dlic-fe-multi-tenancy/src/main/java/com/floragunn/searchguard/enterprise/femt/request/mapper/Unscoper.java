package com.floragunn.searchguard.enterprise.femt.request.mapper;

import org.elasticsearch.action.ActionResponse;

public interface Unscoper<T extends ActionResponse> {
    T unscopeResponse(T scopedResponse);
}
