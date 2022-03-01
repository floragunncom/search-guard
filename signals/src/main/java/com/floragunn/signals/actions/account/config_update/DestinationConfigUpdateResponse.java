package com.floragunn.signals.actions.account.config_update;

import java.io.IOException;
import java.util.List;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

public class DestinationConfigUpdateResponse extends BaseNodesResponse<TransportDestinationConfigUpdateAction.NodeResponse> {

    public DestinationConfigUpdateResponse(StreamInput in) throws IOException {
        super(in);
    }

    public DestinationConfigUpdateResponse(final ClusterName clusterName, List<TransportDestinationConfigUpdateAction.NodeResponse> nodes,
            List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    public List<TransportDestinationConfigUpdateAction.NodeResponse> readNodesFrom(final StreamInput in) throws IOException {
        return in.readList(TransportDestinationConfigUpdateAction.NodeResponse::readNodeResponse);
    }

    @Override
    public void writeNodesTo(final StreamOutput out, List<TransportDestinationConfigUpdateAction.NodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public String toString() {
        return "DestinationConfigUpdateResponse [failures=" + failures() + ", nodes=" + getNodesMap() + "]";
    }

}
