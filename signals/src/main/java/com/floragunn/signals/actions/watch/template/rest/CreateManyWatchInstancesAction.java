package com.floragunn.signals.actions.watch.template.rest;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.UnparsedDocument;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.SearchGuardVersion;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.signals.Signals;
import com.floragunn.signals.actions.watch.template.rest.CreateOrUpdateOneWatchInstanceAction.CreateOrUpdateOneWatchInstanceRequest;
import com.floragunn.signals.actions.watch.template.service.WatchTemplateService;
import com.floragunn.signals.actions.watch.template.service.WatchTemplateServiceFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.floragunn.signals.actions.watch.template.rest.WatchInstanceIdRepresentation.FIELD_TENANT_ID;
import static com.floragunn.signals.actions.watch.template.rest.WatchInstanceIdRepresentation.FIELD_WATCH_ID;

public class CreateManyWatchInstancesAction extends Action<CreateManyWatchInstancesAction.CreateManyWatchInstances, StandardResponse> {

    private static final Logger log = LogManager.getLogger(CreateManyWatchInstancesAction.class);

    public final static String NAME = "cluster:admin:searchguard:tenant:signals:watch/instances/create_many";
    public static final CreateManyWatchInstancesAction INSTANCE = new CreateManyWatchInstancesAction();

    public static final RestApi REST_API = new RestApi().responseHeaders(SearchGuardVersion.header())//
        .handlesPut("/_signals/watch/{tenant}/{id}/instances")//
        .with(INSTANCE, (params, body) -> new CreateManyWatchInstances(params.get("tenant"), params.get("id"), body))//
        .name("PUT /_signals/watch/{tenant}/{id}/instances");

    public CreateManyWatchInstancesAction() {
        super(NAME, CreateManyWatchInstances::new, StandardResponse::new);
    }

    public static class CreateManyWatchInstancesActionHandler extends Handler<CreateManyWatchInstances, StandardResponse> {

        private final WatchTemplateService templateService;

        @Inject
        public CreateManyWatchInstancesActionHandler(HandlerDependencies handlerDependencies, Signals signals, Client client,
            ThreadPool threadPool) {
            super(INSTANCE, handlerDependencies);
            this.templateService = new WatchTemplateServiceFactory(signals, client, threadPool).create();
        }

        @Override
        protected CompletableFuture<StandardResponse> doExecute(CreateManyWatchInstances request) {
            return supplyAsync(() -> {
                try {
                    return templateService.createManyInstances(request);
                } catch (ConfigValidationException e) {
                    log.error("Cannot create watch template instances.", e);
                    return new StandardResponse(500).message("Cannot create watch template instances.").error(e);
                }
            });
        }
    }

    public static class CreateManyWatchInstances extends Request {

        public static final String WATCH_INSTANCES = "watch_instances";
        private final String tenantId;
        private final String watchId;
        private final ImmutableMap<String, Object> watchInstances;

        public CreateManyWatchInstances(String tenantId, String watchId, UnparsedDocument<?> message) throws ConfigValidationException {
            this.tenantId = Objects.requireNonNull(tenantId, "Tenant id is required");
            this.watchId = Objects.requireNonNull(watchId, "Watch id is required");
            if(message == null) {
                ValidationError validationError = new ValidationError("body",
                    "Request body is required and should contains watch instances");
                throw new ConfigValidationException(validationError);
            }
            DocNode docNode = message.parseAsDocNode();
            this.watchInstances = docNode.toMap();
            if(watchInstances.isEmpty()) {
                ValidationError validationError = new ValidationError("body",
                    "Request does not contain any definitions of watch instances.");
                throw new ConfigValidationException(validationError);
            }
        }

        public CreateManyWatchInstances(UnparsedMessage unparsedMessage) throws ConfigValidationException {
            DocNode docNode = unparsedMessage.requiredDocNode();
            this.tenantId = docNode.getAsString(FIELD_TENANT_ID);
            this.watchId = docNode.getAsString(FIELD_WATCH_ID);
            DocNode watchInstancesNode = docNode.getAsNode(WATCH_INSTANCES);
            if (watchInstancesNode == null) {
                ValidationError validationError = new ValidationError("body", "Request does not contain any definitions of watch instances.");
                throw new ConfigValidationException(validationError);
            }
            this.watchInstances = docNode.toMap();
            if (watchInstances.isEmpty()) {
                ValidationError validationError = new ValidationError("body", "Request does not contain any definitions of watch instances.");
                throw new ConfigValidationException(validationError);
            }
        }

        public String getTenantId() {
            return tenantId;
        }

        public String getWatchId() {
            return watchId;
        }

        @Override
        public ImmutableMap<String, Object> toBasicObject() {
            return ImmutableMap.of(FIELD_TENANT_ID, tenantId, FIELD_WATCH_ID, watchId, WATCH_INSTANCES, watchInstances);
        }

        public ImmutableList<CreateOrUpdateOneWatchInstanceRequest> toCreateOneWatchInstanceRequest() {
            List<CreateOrUpdateOneWatchInstanceRequest> mutableList = watchInstances.entrySet()//
                    .stream()//
                    .map(entry -> new CreateOrUpdateOneWatchInstanceRequest(tenantId, watchId, entry.getKey(), DocNode.wrap(entry.getValue()))) //
                    .collect(Collectors.toList());
            return ImmutableList.of(mutableList);
        }

        public ImmutableSet<String> instanceIds() {
            return watchInstances.keySet();
        }
    }
}