package com.floragunn.signals.actions.watch.template.rest;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.UnparsedDocument;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.ValidationError;
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

import java.util.concurrent.CompletableFuture;

public class CreateOneWatchInstanceAction extends Action<CreateOneWatchInstanceAction.CreateOneWatchInstanceRequest, StandardResponse> {

    public final static String NAME = "cluster:admin:searchguard:tenant:signals:watch/instance/create";
    public static final CreateOneWatchInstanceAction INSTANCE = new CreateOneWatchInstanceAction();

    public static final RestApi REST_API = new RestApi()
        .responseHeaders(SearchGuardVersion.header())//
        .handlesPut("/_signals/watch/{tenant}/{id}/instances/{instance_id}")//
        .with(INSTANCE, (params, body) -> new CreateOneWatchInstanceRequest(params.get("tenant"), params.get("id"),
            params.get("instance_id"), body))//
        .name("PUT /_signals/watch/{tenant}/{id}/instances/{instance_id}");

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
        public static final String FIELD_PARAMETERS = "parameters";
        private final TemplateParametersIdRepresentation id;
        private final ImmutableMap<String, Object> parameters;

        public CreateOneWatchInstanceRequest(UnparsedMessage message) throws ConfigValidationException {
            DocNode docNode = message.requiredDocNode();
            this.id = new TemplateParametersIdRepresentation(docNode);
            this.parameters = docNode.getAsNode(FIELD_PARAMETERS).toMap();
        }

        public CreateOneWatchInstanceRequest(String tenantId, String watchId, String instanceId, UnparsedDocument message)
            throws ConfigValidationException {
            this.id = new TemplateParametersIdRepresentation(tenantId, watchId, instanceId);
            if(message == null) {
                ValidationError validationError = new ValidationError("body",
                    "Request body is required and should contains watch template parameters");
                throw new ConfigValidationException(validationError);
            }
            DocNode docNode = message.parseAsDocNode();
            this.parameters = docNode.toMap();
        }

        @Override
        public Object toBasicObject() {
            return this.id.toBasicObject().with(FIELD_PARAMETERS, parameters);
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

        public ImmutableMap<String, Object> getParameters() {
            return parameters;
        }
    }
}