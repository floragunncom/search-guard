package com.floragunn.signals.actions.summary;

import com.floragunn.searchsupport.action.Action;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardResponse;

public class LoadOperatorSummaryAction extends Action<LoadOperatorSummaryRequest, StandardResponse> {

    public static final String NAME = "cluster:admin:searchguard:signals:summary/load";
    public static final LoadOperatorSummaryAction INSTANCE = new LoadOperatorSummaryAction();

    public static final RestApi REST_API = new RestApi()//
        .name("Operation summary")
        .handlesPost("/_signals/watch/{tenant}/summary")
        .with(INSTANCE, (params, body) -> new LoadOperatorSummaryRequest(params.get("tenant"), params.get("sorting"), params.get("size"), body));

    public LoadOperatorSummaryAction() {
        super(NAME, LoadOperatorSummaryRequest::new, StandardResponse::new);
    }
}
