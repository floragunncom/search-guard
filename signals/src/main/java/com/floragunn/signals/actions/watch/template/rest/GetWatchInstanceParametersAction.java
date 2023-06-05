package com.floragunn.signals.actions.watch.template.rest;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.SearchGuardVersion;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardRequests.IdRequest;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.signals.actions.watch.template.service.WatchTemplateService;
import com.floragunn.signals.actions.watch.template.service.persistence.WatchParametersRepository;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class GetWatchInstanceParametersAction extends Action<GetWatchInstanceParametersAction.GetWatchInstanceParametersRequest, StandardResponse> {

    public final static String NAME = "cluster:admin:searchguard:tenant:signals:watch/instance/parameters";
    public static final GetWatchInstanceParametersAction INSTANCE = new GetWatchInstanceParametersAction();

    public static final RestApi REST_API = new RestApi().responseHeaders(SearchGuardVersion.header())//
        .handlesGet("/_signals/watch/{tenant_id}/{watch_id}/instances/{instance_id}/parameters")//
        .with(INSTANCE, (params, body) -> new GetWatchInstanceParametersRequest(params.get("tenant_id"), params.get("watch_id"), params.get("instance_id")))//
        .name("GET /_signals/watch/{tenant_id}/{watch_id}/instances/{instance_id}/parameters");

    public GetWatchInstanceParametersAction() {
        super(NAME, GetWatchInstanceParametersRequest::new, StandardResponse::new);
    }

    public static class GetWatchInstanceParametersHandler extends Handler<GetWatchInstanceParametersRequest, StandardResponse> {
        private final WatchTemplateService templateService;

        @Inject
        public GetWatchInstanceParametersHandler(HandlerDependencies handlerDependencies, Client client) {
            super(INSTANCE, handlerDependencies);
            PrivilegedConfigClient privilegedConfigClient = PrivilegedConfigClient.adapt(client);
            WatchParametersRepository watchParametersRepository = new WatchParametersRepository(privilegedConfigClient);
            this.templateService = new WatchTemplateService(watchParametersRepository);
        }

        @Override
        protected CompletableFuture<StandardResponse> doExecute(GetWatchInstanceParametersRequest request) {
            return supplyAsync(() -> templateService.getTemplateParameters(request));
        }
    }

    public static class GetWatchInstanceParametersRequest extends Request {

        public static final String FIELD_TENANT_ID = "tenant_id";
        public static final String FIELD_WATCH_ID = "watch_id";
        public static final String FIELD_INSTANCE_ID = "instance_id";
        private final String tenantId;
        private final String watchId;
        private final String instanceId;

        public GetWatchInstanceParametersRequest(String tenantId, String watchId, String instanceId) {
            this.tenantId = Objects.requireNonNull(tenantId, "Tenant id is required");
            this.watchId = Objects.requireNonNull(watchId, "Watch id is required");
            this.instanceId = Objects.requireNonNull(instanceId, "Instance id is required");
        }

        public GetWatchInstanceParametersRequest(UnparsedMessage unparsedMessage) throws ConfigValidationException {
            DocNode docNode = unparsedMessage.requiredDocNode();
            this.tenantId = docNode.getAsString(FIELD_TENANT_ID);
            this.watchId = docNode.getAsString(FIELD_WATCH_ID);
            this.instanceId = docNode.getAsString(FIELD_INSTANCE_ID);
        }

        @Override
        public Object toBasicObject() {
            return ImmutableMap.of(FIELD_TENANT_ID, tenantId, FIELD_WATCH_ID, watchId, FIELD_INSTANCE_ID, instanceId);
        }
    }
}