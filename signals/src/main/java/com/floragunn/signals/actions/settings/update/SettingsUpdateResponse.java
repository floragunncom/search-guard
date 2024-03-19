package com.floragunn.signals.actions.settings.update;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class SettingsUpdateResponse extends BaseNodesResponse<TransportSettingsUpdateAction.NodeResponse> {

    public SettingsUpdateResponse(final ClusterName clusterName, List<TransportSettingsUpdateAction.NodeResponse> nodes,
            List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    public SettingsUpdateResponse(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public List<TransportSettingsUpdateAction.NodeResponse> readNodesFrom(final StreamInput in) throws IOException {
        return in.readList(TransportSettingsUpdateAction.NodeResponse::readNodeResponse);
    }

    @Override
    public void writeNodesTo(final StreamOutput out, List<TransportSettingsUpdateAction.NodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public String toString() {
        return "SettingsUpdateResponse [failures=" + failures() + ", nodes=" + getNodesMap() + "]";
    }

}
