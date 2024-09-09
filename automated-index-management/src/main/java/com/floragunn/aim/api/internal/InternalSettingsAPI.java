package com.floragunn.aim.api.internal;

import com.floragunn.aim.AutomatedIndexManagement;
import com.floragunn.aim.AutomatedIndexManagementSettings;
import com.floragunn.codova.documents.DocWriter;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import org.elasticsearch.action.*;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.nodes.*;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class InternalSettingsAPI {
    public final static List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> HANDLERS = ImmutableList.of(
            new ActionPlugin.ActionHandler<>(Update.INSTANCE, Update.Handler.class),
            new ActionPlugin.ActionHandler<>(Refresh.INSTANCE, Refresh.Handler.class));

    private static AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?> readDynamicAttribute(StreamInput input) throws IOException {
        return AutomatedIndexManagementSettings.Dynamic.findAvailableSettingByKey(input.readString());
    }

    private static void writeDynamicAttribute(StreamOutput out, AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?> attribute)
            throws IOException {
        out.writeString(attribute.getName());
    }

    private static Map<AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?>, Object> readDynamicSettingsMap(StreamInput input)
            throws IOException {
        return Objects.requireNonNull(input.readGenericMap()).entrySet().stream().map(entry -> {
            AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?> attribute = AutomatedIndexManagementSettings.Dynamic
                    .findAvailableSettingByKey(entry.getKey());
            return new AbstractMap.SimpleImmutableEntry<AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?>, Object>(attribute,
                    attribute.fromBasicObject(entry.getValue()));
        }).collect(Collectors.toMap(AbstractMap.SimpleImmutableEntry::getKey, AbstractMap.SimpleImmutableEntry::getValue));
    }

    private static void writeDynamicSettingsMap(StreamOutput out, Map<AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?>, Object> settings)
            throws IOException {
        out.writeGenericMap(settings.entrySet().stream().map(entry -> {
            AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?> attribute = entry.getKey();
            return new AbstractMap.SimpleImmutableEntry<>(attribute.getName(), attribute.toBasicObject(entry.getValue()));
        }).collect(Collectors.toMap(AbstractMap.SimpleImmutableEntry::getKey, AbstractMap.SimpleImmutableEntry::getValue)));
    }

    public static class Update extends ActionType<Update.Response> {
        public static final String NAME = "cluster:admin:searchguard:aim:internal:settings:update/post";
        public static final Update INSTANCE = new Update();

        private Update() {
            super(NAME);
        }

        public static class Request extends ActionRequest {
            private final Map<AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?>, Object> changed;
            private final List<AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?>> deleted;

            public Request(Map<AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?>, Object> changed,
                    List<AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?>> deleted) {
                super();
                this.changed = changed;
                this.deleted = deleted;
            }

            public Request(StreamInput input) throws IOException {
                super(input);
                changed = readDynamicSettingsMap(input);
                deleted = input.readCollectionAsList(InternalSettingsAPI::readDynamicAttribute);
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
                writeDynamicSettingsMap(out, changed);
                out.writeCollection(deleted, InternalSettingsAPI::writeDynamicAttribute);
            }

            @Override
            public ActionRequestValidationException validate() {
                return null;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                Request request = (Request) o;
                return Objects.equals(changed, request.changed) && Objects.equals(deleted, request.deleted);
            }

            public Map<AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?>, Object> getChanged() {
                return changed;
            }

            public List<AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?>> getDeleted() {
                return deleted;
            }
        }

        public static class Response extends ActionResponse {
            private final List<AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?>> failedAttributes;
            private final boolean refreshFailed;

            public Response(List<AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?>> failed, boolean refreshFailed) {
                super();
                this.failedAttributes = failed;
                this.refreshFailed = refreshFailed;
            }

            public Response(StreamInput input) throws IOException {
                super(input);
                failedAttributes = input.readCollectionAsList(InternalSettingsAPI::readDynamicAttribute);
                refreshFailed = input.readBoolean();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeCollection(failedAttributes, InternalSettingsAPI::writeDynamicAttribute);
                out.writeBoolean(refreshFailed);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                Response response = (Response) o;
                return refreshFailed == response.refreshFailed && Objects.equals(failedAttributes, response.failedAttributes);
            }

            public List<AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?>> getFailedAttributes() {
                return failedAttributes;
            }

            public boolean hasFailedAttributes() {
                return !failedAttributes.isEmpty();
            }

            public boolean hasRefreshFailures() {
                return refreshFailed;
            }
        }

        public static class Handler extends HandledTransportAction<Request, Response> {
            private final Client client;

            @Inject
            public Handler(Client client, TransportService transportService, ActionFilters actionFilters, ThreadPool threadPool) {
                super(NAME, transportService, actionFilters, Request::new, threadPool.generic());
                this.client = client;
            }

            @Override
            protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
                BulkRequest bulkRequest = new BulkRequest(AutomatedIndexManagementSettings.ConfigIndices.SETTINGS_NAME);
                for (AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?> attribute : request.getDeleted()) {
                    bulkRequest.add(new DeleteRequest().id(attribute.getName()));
                }
                for (Map.Entry<AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?>, Object> entry : request.getChanged().entrySet()) {
                    bulkRequest.add(new IndexRequest().id(entry.getKey().getName())
                            .source(ImmutableMap.of("setting", DocWriter.json().writeAsString(entry.getKey().toBasicObject(entry.getValue())))));
                }
                client.bulk(bulkRequest, new ActionListener<>() {
                    @Override
                    public void onResponse(BulkResponse bulkItemResponses) {
                        Map<AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?>, Object> changed = request.getChanged();
                        List<AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?>> deleted = request.getDeleted();
                        List<AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?>> failed = new ArrayList<>();
                        if (bulkItemResponses.hasFailures()) {
                            changed = new HashMap<>(request.getChanged());
                            deleted = new ArrayList<>(request.getDeleted());
                            for (BulkItemResponse item : bulkItemResponses.getItems()) {
                                if (item.isFailed()) {
                                    AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?> failedAttribute = AutomatedIndexManagementSettings.Dynamic
                                            .findAvailableSettingByKey(item.getId());
                                    if (DocWriteRequest.OpType.DELETE.equals(item.getOpType())) {
                                        deleted.remove(failedAttribute);
                                    } else {
                                        changed.remove(failedAttribute);
                                    }
                                    failed.add(failedAttribute);
                                }
                            }
                        }
                        client.execute(Refresh.INSTANCE, new Refresh.Request(changed, deleted), new ActionListener<>() {
                            @Override
                            public void onResponse(Refresh.Response response) {
                                listener.onResponse(new Response(failed, response.hasFailures()));
                            }

                            @Override
                            public void onFailure(Exception e) {
                                listener.onFailure(e);
                            }
                        });
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                });
            }
        }
    }

    public static class Refresh extends ActionType<Refresh.Response> {
        public static final String NAME = "cluster:admin:searchguard:aim:internal:settings:refresh/post";
        public static final Refresh INSTANCE = new Refresh();

        private Refresh() {
            super(NAME);
        }

        public static class Request extends BaseNodesRequest<Request> {
            private final Map<AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?>, Object> changed;
            private final List<AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?>> deleted;

            public Request(Map<AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?>, Object> changed,
                    List<AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?>> deleted) {
                super((String[]) null);
                this.changed = changed;
                this.deleted = deleted;
            }

            protected Request(StreamInput input) throws IOException {
                super(input);
                changed = readDynamicSettingsMap(input);
                deleted = input.readCollectionAsList(InternalSettingsAPI::readDynamicAttribute);
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
                writeDynamicSettingsMap(out, changed);
                out.writeCollection(deleted, InternalSettingsAPI::writeDynamicAttribute);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                Request request = (Request) o;
                return Objects.equals(changed, request.changed) && Objects.equals(deleted, request.deleted);
            }

            public Map<AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?>, Object> getChanged() {
                return changed;
            }

            public List<AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?>> getDeleted() {
                return deleted;
            }

            public static class Node extends TransportRequest {
                private final Request request;

                public Node(Request request) {
                    this.request = request;
                }

                protected Node(StreamInput input) throws IOException {
                    super(input);
                    request = new Request(input);
                }

                @Override
                public void writeTo(StreamOutput out) throws IOException {
                    super.writeTo(out);
                    request.writeTo(out);
                }

                @Override
                public boolean equals(Object o) {
                    if (this == o) {
                        return true;
                    }
                    if (o == null || getClass() != o.getClass()) {
                        return false;
                    }
                    Node node = (Node) o;
                    return Objects.equals(request, node.request);
                }

                public Request getRequest() {
                    return request;
                }
            }
        }

        public static class Response extends BaseNodesResponse<Response.Node> {
            public Response(ClusterName clusterName, List<Response.Node> nodeResponses, List<FailedNodeException> failed) {
                super(clusterName, nodeResponses, failed);
            }

            public Response(StreamInput input) throws IOException {
                super(input);
            }

            @Override
            protected List<Node> readNodesFrom(StreamInput input) throws IOException {
                return input.readCollectionAsList(Node::new);
            }

            @Override
            protected void writeNodesTo(StreamOutput out, List<Node> nodes) throws IOException {
                out.writeCollection(nodes);
            }

            @Override
            public boolean equals(Object obj) {
                return obj instanceof Response;
            }

            public static class Node extends BaseNodeResponse {
                public Node(DiscoveryNode discoveryNode) {
                    super(discoveryNode);
                }

                protected Node(StreamInput input) throws IOException {
                    super(input);
                }
            }
        }

        public static class Handler extends TransportNodesAction<Request, Response, Request.Node, Response.Node> {
            private final AutomatedIndexManagement aim;

            @Inject
            public Handler(AutomatedIndexManagement aim, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                    ActionFilters actionFilters) {
                super(NAME, clusterService, transportService, actionFilters, Request.Node::new, threadPool.executor(ThreadPool.Names.MANAGEMENT));
                this.aim = aim;
            }

            @Override
            protected Response newResponse(Request request, List<Response.Node> nodes, List<FailedNodeException> failures) {
                return new Response(clusterService.getClusterName(), nodes, failures);
            }

            @Override
            protected Request.Node newNodeRequest(Request request) {
                return new Request.Node(request);
            }

            @Override
            protected Response.Node newNodeResponse(StreamInput input, DiscoveryNode node) throws IOException {
                return new Response.Node(input);
            }

            @Override
            protected Response.Node nodeOperation(Request.Node request, Task task) {
                aim.getAimSettings().getDynamic().refresh(request.getRequest().getChanged(), request.getRequest().getDeleted());
                return new Response.Node(clusterService.localNode());
            }
        }
    }
}
