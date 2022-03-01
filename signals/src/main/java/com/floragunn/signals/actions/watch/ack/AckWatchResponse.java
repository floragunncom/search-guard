
package com.floragunn.signals.actions.watch.ack;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

public class AckWatchResponse extends BaseNodesResponse<TransportAckWatchAction.NodeResponse> {
    private final static Logger log = LogManager.getLogger(AckWatchResponse.class);

    private Status status;
    private String statusMessage;

    public AckWatchResponse(final ClusterName clusterName, List<TransportAckWatchAction.NodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    public AckWatchResponse(StreamInput in) throws IOException {
        super(in);

        initStatus();
    }

    @Override
    public List<TransportAckWatchAction.NodeResponse> readNodesFrom(final StreamInput in) throws IOException {
        return in.readList(TransportAckWatchAction.NodeResponse::readNodeResponse);
    }

    @Override
    public void writeNodesTo(final StreamOutput out, List<TransportAckWatchAction.NodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public String toString() {
        return "AckWatchResponse [failures=" + failures() + ", nodes=" + getNodesMap() + "]";
    }

    private void initStatus() {
        if (log.isDebugEnabled()) {
            log.debug("AckWatch node responses: " + getNodes());
        }
        
        TransportAckWatchAction.NodeResponse nodeResponse = getResponsibleNodeResponse();

        if (nodeResponse == null) {
            this.status = Status.NO_SUCH_WATCH;
            this.statusMessage = "Could not find watch";
            return;
        }

        this.status = nodeResponse.getStatus();
        this.statusMessage = nodeResponse.getMessage();
    }

    private TransportAckWatchAction.NodeResponse getResponsibleNodeResponse() {
        for (TransportAckWatchAction.NodeResponse nodeResponse : getNodes()) {
            if (nodeResponse.getStatus() == Status.SUCCESS) {
                return nodeResponse;
            }
        }

        for (TransportAckWatchAction.NodeResponse nodeResponse : getNodes()) {
            if (nodeResponse.getStatus() == Status.ILLEGAL_STATE) {
                return nodeResponse;
            }
        }

        for (TransportAckWatchAction.NodeResponse nodeResponse : getNodes()) {
            if (nodeResponse.getStatus() == Status.EXCEPTION) {
                return nodeResponse;
            }
        }

        return null;
    }

    public static enum Status {
        SUCCESS, NO_SUCH_WATCH, EXCEPTION, ILLEGAL_STATE, NO_SUCH_TENANT, UNAUTHORIZED
    }

    public Status getStatus() {
        if (status == null) {
            initStatus();
        }

        return status;
    }

    public String getStatusMessage() {
        if (status == null) {
            initStatus();
        }

        return statusMessage;
    }

}
