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

public class DeleteWatchInstanceAction extends Action<DeleteWatchInstanceAction.DeleteWatchInstanceRequest, StandardResponse> {

    public final static String NAME = "cluster:admin:searchguard:tenant:signals:watch/instances/delete";
    public static final DeleteWatchInstanceAction INSTANCE = new DeleteWatchInstanceAction();

    public static final RestApi REST_API = new RestApi().responseHeaders(SearchGuardVersion.header())//
        .handlesDelete("/_signals/watch/{tenant}/{id}/instances/{instance_id}")//
        .with(INSTANCE, (params, body) -> new DeleteWatchInstanceRequest(params.get("tenant"), params.get("id"), params.get("instance_id")))//
        .name("DELETE /_signals/watch/{tenant}/{id}/instances/{instance_id}");

    public DeleteWatchInstanceAction() {
        super(NAME, DeleteWatchInstanceRequest::new, StandardResponse::new);
    }

    public static class DeleteWatchInstanceHandler extends Handler<DeleteWatchInstanceRequest, StandardResponse> {

        private final GenericWatchService genericWatchService;

        @Inject
        public DeleteWatchInstanceHandler(HandlerDependencies handlerDependencies, Signals signals, Client client, ThreadPool threadPool) {
            super(INSTANCE, handlerDependencies);
            this.genericWatchService = new GenericWatchServiceFactory(signals, client, threadPool).create();
        }

        @Override
        protected CompletableFuture<StandardResponse> doExecute(DeleteWatchInstanceRequest request) {
            return supplyAsync(() -> genericWatchService.deleteWatchInstance(request));
        }
    }

    public static class DeleteWatchInstanceRequest extends Request {

        private final WatchInstanceIdRepresentation id;

        public DeleteWatchInstanceRequest(String tenantId, String watchId, String instanceId) {
            this.id = new WatchInstanceIdRepresentation(tenantId, watchId, instanceId);
        }

        public DeleteWatchInstanceRequest(UnparsedMessage unparsedMessage) throws ConfigValidationException {
            DocNode docNode = unparsedMessage.requiredDocNode();
            this.id = new WatchInstanceIdRepresentation(docNode);
        }

        @Override
        public Object toBasicObject() {
            return id.toBasicObject();
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