package com.floragunn.signals.actions.watch.template.rest;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.UnparsedDocument;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.SearchGuardVersion;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.signals.actions.watch.template.service.WatchTemplateService;
import com.floragunn.signals.actions.watch.template.service.persistence.WatchParametersRepository;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class CreateOneWatchInstanceAction extends Action<CreateOneWatchInstanceAction.CreateOneWatchInstanceRequest, StandardResponse> {

    public final static String NAME = "cluster:admin:searchguard:tenant:signals:watch/instance/create";
    public static final CreateOneWatchInstanceAction INSTANCE = new CreateOneWatchInstanceAction();

    public static final RestApi REST_API = new RestApi()
        .responseHeaders(SearchGuardVersion.header())//
        .handlesPut("/_signals/watch/{tenant_id}/{watch_id}/instances/{instance_id}")//
        .with(INSTANCE, (params, body) -> new CreateOneWatchInstanceRequest(params.get("tenant_id"), params.get("watch_id"),
            params.get("instance_id"), body))//
        .name("PUT /_signals/watch/{tenant}/{watch_id}/instances/{instance_id}");

    public CreateOneWatchInstanceAction() {
        super(NAME, CreateOneWatchInstanceRequest::new, StandardResponse::new);
    }

    public static class CreateWatchInstanceHandler extends Handler<CreateOneWatchInstanceRequest, StandardResponse> {

        private final WatchTemplateService templateService;

        @Inject
        public CreateWatchInstanceHandler(NodeClient client, HandlerDependencies handlerDependencies) {
            super(INSTANCE, handlerDependencies);
            PrivilegedConfigClient privilegedConfigClient = PrivilegedConfigClient.adapt(client);
            WatchParametersRepository watchParametersRepository = new WatchParametersRepository(privilegedConfigClient);
            this.templateService = new WatchTemplateService(watchParametersRepository);
        }

        @Override
        protected CompletableFuture<StandardResponse> doExecute(CreateOneWatchInstanceRequest request) {
            return supplyAsync(() -> templateService.createOrUpdate(request));
        }
    }

    public static class CreateOneWatchInstanceRequest extends Request {

        public static final String FIELD_TENANT_ID = "tenant_id";
        public static final String FIELD_WATCH_ID = "watch_id";
        public static final String FIELD_INSTANCE_ID = "instance_id";
        public static final String FIELD_PARAMETERS = "parameters";
        private final String tenantId;
        private final String watchId;
        private final String instanceId;
        private final ImmutableMap<String, Object> parameters;

        public CreateOneWatchInstanceRequest(UnparsedMessage message) throws ConfigValidationException {
            DocNode docNode = message.requiredDocNode();
            this.tenantId = docNode.getAsString(FIELD_TENANT_ID);
            this.watchId = docNode.getAsString(FIELD_WATCH_ID);
            this.instanceId = docNode.getAsString(FIELD_INSTANCE_ID);
            this.parameters = docNode.getAsNode(FIELD_PARAMETERS).toMap();
        }

        public CreateOneWatchInstanceRequest(String tenantId, String watchId, String instanceId, UnparsedDocument message)
            throws ConfigValidationException {
            this.tenantId = Objects.requireNonNull(tenantId, "tenant id is required");
            this.watchId = Objects.requireNonNull(watchId, "Watch id is required");
            this.instanceId = Objects.requireNonNull(instanceId, "Instance id is required");
            DocNode docNode = message.parseAsDocNode();
            this.parameters = docNode.toMap();
        }

        @Override
        public Object toBasicObject() {
            return ImmutableMap.of(FIELD_TENANT_ID, tenantId, FIELD_WATCH_ID, watchId, FIELD_INSTANCE_ID, instanceId,
                FIELD_PARAMETERS, parameters);
        }

        public String getTenantId() {
            return tenantId;
        }

        public String getWatchId() {
            return watchId;
        }

        public String getInstanceId() {
            return instanceId;
        }

        public ImmutableMap<String, Object> getParameters() {
            return parameters;
        }
    }
}