package com.floragunn.signals.actions.watch;

import com.floragunn.searchguard.SearchGuardVersion;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardRequests.IdRequest;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.searchsupport.action.Action;
import org.elasticsearch.common.inject.Inject;

import java.util.concurrent.CompletableFuture;

public class CreateWatchInstanceAction extends Action<IdRequest , StandardResponse> {

    public final static String NAME = "cluster:admin:searchguard:tenant:signals:watch/instance/create";
    public static final CreateWatchInstanceAction INSTANCE = new CreateWatchInstanceAction();

    public static final RestApi REST_API = new RestApi()
        .responseHeaders(SearchGuardVersion.header())//
        .handlesPut("/_signals/watch/{tenant}/{watch_id}/instances/{instance_id}")
        .with(INSTANCE, (params, body) -> new IdRequest(params.get("tenant")))
        .name("PUT /_signals/watch/{tenant}/{watch_id}/instances/{instance_id}");

    public CreateWatchInstanceAction() {
        super(NAME, IdRequest::new, StandardResponse::new);
    }

    public static class CreateWatchInstanceHandler extends Handler<IdRequest, StandardResponse> {
        @Inject
        public CreateWatchInstanceHandler(HandlerDependencies handlerDependencies) {
            super(INSTANCE, handlerDependencies);
        }

        @Override
        protected CompletableFuture<StandardResponse> doExecute(IdRequest request) {
            return supplyAsync(() -> {
                return new StandardResponse(303).message("Not implemented yet.");
            });
        }
    }
}