
package com.floragunn.signals.actions.watch.ackandget;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import static java.util.Objects.nonNull;

public class AckAndGetWatchResponse extends BaseNodesResponse<TransportAckAndGetWatchAction.NodeResponse> {
    private final static Logger log = LogManager.getLogger(AckAndGetWatchResponse.class);

    private Status status;
    private String statusMessage;

    private Acknowledgement[] acknowledgements;

    private String[] unackedActionIds;

    public AckAndGetWatchResponse(final ClusterName clusterName, List<TransportAckAndGetWatchAction.NodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
        this.acknowledgements = nodes.stream()
            .filter(response -> nonNull(response.getAcknowledgements()))
            .flatMap(response -> Arrays.stream(response.getAcknowledgements()))
            .toArray(size -> new Acknowledgement[size]);
        this.unackedActionIds = nodes.stream()
            .filter(response -> nonNull(response.getUnackedActionIds()))
            .flatMap(ids -> Arrays.stream(ids.getUnackedActionIds()))
            .toArray(size -> new String[size]);
    }

    public AckAndGetWatchResponse(StreamInput in) throws IOException {
        super(in);

        initStatus();
    }

    public Acknowledgement[] getAcknowledgements() {
        return acknowledgements;
    }

    public Acknowledgement[] getSortedAcknowledgements(Comparator<? super Acknowledgement> comparator) {
        return Arrays.stream(acknowledgements)//
            .sorted(comparator)//
            .toArray(size -> new Acknowledgement[size]);
    }

    public String[] getUnackedActionIds() {
        return unackedActionIds;
    }

    @Override
    public List<TransportAckAndGetWatchAction.NodeResponse> readNodesFrom(final StreamInput in) throws IOException {
        return in.readList(TransportAckAndGetWatchAction.NodeResponse::readNodeResponse);
    }

    @Override
    public void writeNodesTo(final StreamOutput out, List<TransportAckAndGetWatchAction.NodeResponse> nodes) throws IOException {
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
        
        TransportAckAndGetWatchAction.NodeResponse nodeResponse = getResponsibleNodeResponse();

        if (nodeResponse == null) {
            this.status = Status.NO_SUCH_WATCH;
            this.statusMessage = "Could not find watch";
            return;
        }

        this.status = nodeResponse.getStatus();
        this.statusMessage = nodeResponse.getMessage();
        this.acknowledgements = nodeResponse.getAcknowledgements();
        this.unackedActionIds = nodeResponse.getUnackedActionIds();
    }

    private TransportAckAndGetWatchAction.NodeResponse getResponsibleNodeResponse() {
        for (TransportAckAndGetWatchAction.NodeResponse nodeResponse : getNodes()) {
            if (nodeResponse.getStatus() == Status.SUCCESS) {
                return nodeResponse;
            }
        }

        for (TransportAckAndGetWatchAction.NodeResponse nodeResponse : getNodes()) {
            if (nodeResponse.getStatus() == Status.ILLEGAL_STATE || nodeResponse.getStatus() == Status.NO_SUCH_ACTION || nodeResponse.getStatus() == Status.NOT_ACKNOWLEDGEABLE || nodeResponse.getStatus() == Status.NO_SUCH_TENANT) {
                return nodeResponse;
            }
        }

        for (TransportAckAndGetWatchAction.NodeResponse nodeResponse : getNodes()) {
            if (nodeResponse.getStatus() == Status.EXCEPTION || nodeResponse.getStatus() == Status.UNAUTHORIZED) {
                return nodeResponse;
            }
        }

        return null;
    }

    public static enum Status {
        SUCCESS, NO_SUCH_WATCH, EXCEPTION, ILLEGAL_STATE, NO_SUCH_TENANT, UNAUTHORIZED, NO_SUCH_ACTION, NOT_ACKNOWLEDGEABLE
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
