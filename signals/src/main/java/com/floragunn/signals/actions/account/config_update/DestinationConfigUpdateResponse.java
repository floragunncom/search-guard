package com.floragunn.signals.actions.account.config_update;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

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
        return in.readCollectionAsList(TransportDestinationConfigUpdateAction.NodeResponse::readNodeResponse);
    }

    @Override
    public void writeNodesTo(final StreamOutput out, List<TransportDestinationConfigUpdateAction.NodeResponse> nodes) throws IOException {
        out.writeCollection(nodes);
    }

    @Override
    public String toString() {
        return "DestinationConfigUpdateResponse [failures=" + failures() + ", nodes=" + getNodesMap() + "]";
    }

}
