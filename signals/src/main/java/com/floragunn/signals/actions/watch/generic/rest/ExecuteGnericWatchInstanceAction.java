package com.floragunn.signals.actions.watch.generic.rest;

import com.floragunn.searchguard.SearchGuardVersion;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardRequests.IdRequest;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.searchsupport.action.Action;
import org.elasticsearch.common.inject.Inject;

import java.util.concurrent.CompletableFuture;

public class ExecuteGnericWatchInstanceAction extends Action<IdRequest, StandardResponse> {

    //TODO update action name
    public final static String NAME = "cluster:admin:searchguard:tenant:signals:watch/instances/execute";
    public static final ExecuteGnericWatchInstanceAction INSTANCE = new ExecuteGnericWatchInstanceAction();

    public static final RestApi REST_API = new RestApi().responseHeaders(SearchGuardVersion.header())//
        //TODO update REST method and path
        .handlesPost("/_signals/watch/{tenant}/{id}/instances/{instance_id}")//
        .with(INSTANCE, (params, body) -> new IdRequest(params.get("id")))//
        // TODO update HTTP method and request path if necessery
        .name("POST /_signals/watch/{tenant}/{id}/instances/{instance_id}");

    public ExecuteGnericWatchInstanceAction() {
        super(NAME, IdRequest::new, StandardResponse::new);
    }

    public static class ExecuteGnericWatchInstanceHandler extends Handler<IdRequest, StandardResponse> {
        @Inject public ExecuteGnericWatchInstanceHandler(HandlerDependencies handlerDependencies) {
            super(INSTANCE, handlerDependencies);
        }

        @Override protected CompletableFuture<StandardResponse> doExecute(IdRequest request) {
            return supplyAsync(() -> {
                return new StandardResponse(303).message("Not implemented yet.");
            });
        }
    }
}