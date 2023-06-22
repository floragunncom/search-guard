package com.floragunn.signals.actions.watch.template.rest;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.SearchGuardVersion;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.signals.Signals;
import com.floragunn.signals.actions.watch.template.service.WatchTemplateService;
import com.floragunn.signals.actions.watch.template.service.WatchTemplateServiceFactory;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.CompletableFuture;

public class GetWatchInstanceParametersAction extends Action<GetWatchInstanceParametersAction.GetWatchInstanceParametersRequest, StandardResponse> {

    public final static String NAME = "cluster:admin:searchguard:tenant:signals:watch/instances/parameters/get";
    public static final GetWatchInstanceParametersAction INSTANCE = new GetWatchInstanceParametersAction();

    public static final RestApi REST_API = new RestApi().responseHeaders(SearchGuardVersion.header())//
        .handlesGet("/_signals/watch/{tenant}/{id}/instances/{instance_id}/parameters")//
        .with(INSTANCE, (params, body) -> new GetWatchInstanceParametersRequest(params.get("tenant"), params.get("id"), params.get("instance_id")))//
        .name("GET /_signals/watch/{tenant}/{id}/instances/{instance_id}/parameters");

    public GetWatchInstanceParametersAction() {
        super(NAME, GetWatchInstanceParametersRequest::new, StandardResponse::new);
    }

    public static class GetWatchInstanceParametersHandler extends Handler<GetWatchInstanceParametersRequest, StandardResponse> {
        private final WatchTemplateService templateService;

        @Inject
        public GetWatchInstanceParametersHandler(HandlerDependencies dependencies, Signals signals, Client client, ThreadPool threadPool) {
            super(INSTANCE, dependencies);
            this.templateService = new WatchTemplateServiceFactory(signals, client, threadPool).create();
        }

        @Override
        protected CompletableFuture<StandardResponse> doExecute(GetWatchInstanceParametersRequest request) {
            return supplyAsync(() -> templateService.getTemplateParameters(request));
        }
    }

    public static class GetWatchInstanceParametersRequest extends Request {

        private final WatchInstanceIdRepresentation id;

        public GetWatchInstanceParametersRequest(String tenantId, String watchId, String instanceId) {
            this.id = new WatchInstanceIdRepresentation(tenantId, watchId, instanceId);
        }

        public GetWatchInstanceParametersRequest(UnparsedMessage unparsedMessage) throws ConfigValidationException {
            DocNode docNode = unparsedMessage.requiredDocNode();
            this.id = new WatchInstanceIdRepresentation(docNode);
        }

        @Override
        public Object toBasicObject() {
            return this.id.toBasicObject();
        }

        public String getTenantId() {
            return id.getTenantId();
        }

        public String getWatchId() {
            return id.getWatchId();
        }

        public String getInstanceId() {
            return id.getInstanceId();
        }
    }
}