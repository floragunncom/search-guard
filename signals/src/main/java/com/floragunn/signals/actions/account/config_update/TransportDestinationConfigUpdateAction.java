package com.floragunn.signals.actions.account.config_update;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.BaseNodeRequest;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.client.Client;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import com.floragunn.signals.Signals;

public class TransportDestinationConfigUpdateAction extends
        TransportNodesAction<DestinationConfigUpdateRequest, DestinationConfigUpdateResponse, TransportDestinationConfigUpdateAction.NodeRequest, TransportDestinationConfigUpdateAction.NodeResponse> {

    private final static Logger log = LogManager.getLogger(TransportDestinationConfigUpdateAction.class);
    final Client client;
    private final Signals signals;

    @Inject
    public TransportDestinationConfigUpdateAction(Signals signals, final Settings settings, final ThreadPool threadPool,
            final ClusterService clusterService, final TransportService transportService, final ActionFilters actionFilters, final Client client) {
        super(DestinationConfigUpdateAction.NAME, threadPool, clusterService, transportService, actionFilters, DestinationConfigUpdateRequest::new,
                TransportDestinationConfigUpdateAction.NodeRequest::new, ThreadPool.Names.MANAGEMENT,
                TransportDestinationConfigUpdateAction.NodeResponse.class);

        this.client = client;
        this.signals = signals;

    }

    @Override
    protected DestinationConfigUpdateResponse newResponse(DestinationConfigUpdateRequest request, List<NodeResponse> responses,
            List<FailedNodeException> failures) {
        return new DestinationConfigUpdateResponse(this.clusterService.getClusterName(), responses, failures);

    }

    @Override
    protected NodeResponse nodeOperation(final NodeRequest request) {
        DiscoveryNode localNode = clusterService.localNode();

        try {
            signals.getAccountRegistry().updateAtomic(client);
            return new NodeResponse(localNode, NodeResponse.Status.SUCCESS, "");
        } catch (Exception e) {
            log.error("Error while updating destinations", e);
            return new NodeResponse(localNode, NodeResponse.Status.EXCEPTION, e.toString());
        }
    }

    public static class NodeRequest extends BaseNodeRequest {

        DestinationConfigUpdateRequest request;

        public NodeRequest(final DestinationConfigUpdateRequest request) {
            this.request = request;
        }

        public NodeRequest(final StreamInput in) throws IOException {
            super(in);
            request = new DestinationConfigUpdateRequest(in);
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

        public NodeResponse(final DiscoveryNode node, Status status, String message) {
            super(node);
            this.status = status;
            this.message = message;
        }

        public NodeResponse(StreamInput in) throws IOException {
            super(in);
            status = in.readEnum(Status.class);
            message = in.readOptionalString();
        }

        public static TransportDestinationConfigUpdateAction.NodeResponse readNodeResponse(StreamInput in) throws IOException {
            TransportDestinationConfigUpdateAction.NodeResponse result = new TransportDestinationConfigUpdateAction.NodeResponse(in);
            return result;
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

        public static enum Status {
            SUCCESS, EXCEPTION
        }
    }

    @Override
    protected NodeRequest newNodeRequest(DestinationConfigUpdateRequest request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new NodeResponse(in);
    }

}
