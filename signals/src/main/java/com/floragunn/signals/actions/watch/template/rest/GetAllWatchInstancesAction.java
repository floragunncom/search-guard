package com.floragunn.signals.actions.watch.template.rest;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableMap;
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

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static com.floragunn.signals.actions.watch.template.rest.WatchInstanceIdRepresentation.FIELD_TENANT_ID;
import static com.floragunn.signals.actions.watch.template.rest.WatchInstanceIdRepresentation.FIELD_WATCH_ID;

public class GetAllWatchInstancesAction extends Action<GetAllWatchInstancesAction.GetAllWatchInstancesRequest, StandardResponse> {

    public final static String NAME = "cluster:admin:searchguard:tenant:signals:watch/instances";
    public static final GetAllWatchInstancesAction INSTANCE = new GetAllWatchInstancesAction();

    public static final RestApi REST_API = new RestApi().responseHeaders(SearchGuardVersion.header())//
        .handlesGet("/_signals/watch/{tenant}/{id}/instances")//
        .with(INSTANCE, (params, body) -> new GetAllWatchInstancesRequest(params.get("tenant"), params.get("id")))//
        .name("GET /_signals/watch/{tenant}/{id}/instances");

    public GetAllWatchInstancesAction() {
        super(NAME, GetAllWatchInstancesRequest::new, StandardResponse::new);
    }


    public static class GetAllWatchInstancesHandler extends Handler<GetAllWatchInstancesRequest, StandardResponse> {

        private final WatchTemplateService templateService;

        @Inject
        public GetAllWatchInstancesHandler(HandlerDependencies dependencies, Signals signals, Client client, ThreadPool threadPool) {
            super(INSTANCE, dependencies);
            this.templateService = new WatchTemplateServiceFactory(signals, client, threadPool).create();
        }

        @Override
        protected CompletableFuture<StandardResponse> doExecute(GetAllWatchInstancesRequest request) {
            return supplyAsync(() -> templateService.findAllInstances(request));
        }
    }

    public static class GetAllWatchInstancesRequest extends Request {

        private final String tenantId;
        private final String watchId;

        public GetAllWatchInstancesRequest(String tenantId, String watchId) {
            this.tenantId = Objects.requireNonNull(tenantId, "Tenant id is required");
            this.watchId = Objects.requireNonNull(watchId, "Watch id is required");
        }

        public GetAllWatchInstancesRequest(UnparsedMessage unparsedMessage) throws ConfigValidationException {
            DocNode docNode = unparsedMessage.requiredDocNode();
            this.tenantId = docNode.getAsString(FIELD_TENANT_ID);
            this.watchId = docNode.getAsString(FIELD_WATCH_ID);
        }

        @Override
        public ImmutableMap<String, String> toBasicObject() {
            return ImmutableMap.of(FIELD_TENANT_ID, tenantId, FIELD_WATCH_ID, watchId);
        }

        public String getTenantId() {
            return tenantId;
        }

        public String getWatchId() {
            return watchId;
        }
    }
}