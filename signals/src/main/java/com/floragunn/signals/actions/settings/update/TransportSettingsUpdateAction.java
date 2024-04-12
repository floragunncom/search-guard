package com.floragunn.signals.actions.settings.update;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.floragunn.signals.Signals;

public class TransportSettingsUpdateAction extends
        TransportNodesAction<SettingsUpdateRequest, SettingsUpdateResponse, TransportSettingsUpdateAction.NodeRequest, TransportSettingsUpdateAction.NodeResponse> {

    private final static Logger log = LogManager.getLogger(TransportSettingsUpdateAction.class);

    private final Signals signals;
    private final Client client;

    @Inject
    public TransportSettingsUpdateAction(Signals signals, final Settings settings, final ThreadPool threadPool, final ClusterService clusterService,
            final TransportService transportService, final ActionFilters actionFilters, final Client client) {
        super(SettingsUpdateAction.NAME, clusterService, transportService, actionFilters,
                TransportSettingsUpdateAction.NodeRequest::new, threadPool.executor(ThreadPool.Names.MANAGEMENT));

        this.signals = signals;
        this.client = client;

    }

    @Override
    protected NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeResponse(in);
    }

    @Override
    protected SettingsUpdateResponse newResponse(SettingsUpdateRequest request, List<NodeResponse> responses, List<FailedNodeException> failures) {
        return new SettingsUpdateResponse(this.clusterService.getClusterName(), responses, failures);

    }

    @Override
    protected NodeResponse nodeOperation(final NodeRequest request, Task task) {
        DiscoveryNode localNode = clusterService.localNode();

        try {
            signals.getSignalsSettings().refresh(client);

            return new NodeResponse(localNode, NodeResponse.Status.SUCCESS, "");
        } catch (Exception e) {
            log.error("Error while updating settings", e);
            return new NodeResponse(localNode, NodeResponse.Status.EXCEPTION, e.toString());
        }
    }

    public static class NodeRequest extends BaseNodesRequest {

        SettingsUpdateRequest request;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            request = new SettingsUpdateRequest(in);
        }

        public NodeRequest(final SettingsUpdateRequest request) {
            super((String[]) null);
            this.request = request;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }

    public static class NodeResponse extends BaseNodeResponse {

        private Status status;
        private String message;

        public NodeResponse(StreamInput in) throws IOException {
            super(in);
            status = in.readEnum(Status.class);
            message = in.readOptionalString();
        }

        public NodeResponse(final DiscoveryNode node, Status status, String message) {
            super(node);
            this.status = status;
            this.message = message;
        }

        public String getMessage() {
            return message;
        }

        public Status getStatus() {
            return status;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeEnum(status);
            out.writeOptionalString(message);
        }

        @Override
        public String toString() {
            return "NodeResponse [status=" + status + ", message=" + message + "]";
        }

        
        public static NodeResponse readNodeResponse(StreamInput in) throws IOException {
            NodeResponse result = new NodeResponse(in);
            return result;
        }
        
        public static enum Status {
            SUCCESS, EXCEPTION
        }
    }

    @Override
    protected NodeRequest newNodeRequest(SettingsUpdateRequest request) {
        return new NodeRequest(request);
    }

}
