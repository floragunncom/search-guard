package com.floragunn.signals.actions.watch.generic.rest;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.SearchGuardVersion;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.signals.Signals;
import com.floragunn.signals.actions.watch.generic.service.GenericWatchService;
import com.floragunn.signals.actions.watch.generic.service.GenericWatchServiceFactory;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.CompletableFuture;

public class EnableGenericWatchInstanceAction extends Action<EnableGenericWatchInstanceAction.EnableGenericWatchInstanceRequest, StandardResponse> {

    public final static String NAME = "cluster:admin:searchguard:tenant:signals:watch/instances/enable";
    public static final EnableGenericWatchInstanceAction INSTANCE = new EnableGenericWatchInstanceAction();

    public static final RestApi REST_API = new RestApi().responseHeaders(SearchGuardVersion.header())//
        .handlesPut("/_signals/watch/{tenant}/{id}/instances/{instance_id}/_active")//
        .with(INSTANCE, (params, body) -> new EnableGenericWatchInstanceRequest(params.get("tenant"), params.get("id"), params.get("instance_id")))//
        .name("PUT /_signals/watch/{tenant}/{id}/instances/{instance_id}/_active");

    public EnableGenericWatchInstanceAction() {
        super(NAME, EnableGenericWatchInstanceRequest::new, StandardResponse::new);
    }

    public static class EnableGenericWatchInstanceHandler extends Handler<EnableGenericWatchInstanceRequest, StandardResponse> {

        private final GenericWatchService genericWatchService;

        @Inject
        public EnableGenericWatchInstanceHandler(HandlerDependencies handlerDependencies, Signals signals, Client client,
            ThreadPool threadPool) {
            super(INSTANCE, handlerDependencies);
            this.genericWatchService = new GenericWatchServiceFactory(signals, client, threadPool).create();
        }

        @Override
        protected CompletableFuture<StandardResponse> doExecute(EnableGenericWatchInstanceRequest request) {
            return supplyAsync(() -> genericWatchService.switchEnabledFlag(request.getTenantId(), request.getWatchId(), request.getInstanceId(), true));
        }
    }

    public static class EnableGenericWatchInstanceRequest extends Request {

        private final WatchInstanceIdRepresentation id;

        public EnableGenericWatchInstanceRequest(String tenantId, String watchId, String instanceId) {
            this.id = new WatchInstanceIdRepresentation(tenantId, watchId, instanceId);
        }

        public EnableGenericWatchInstanceRequest(UnparsedMessage unparsedMessage) throws ConfigValidationException {
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