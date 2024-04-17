package com.floragunn.searchsupport.jobs.actions;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.quartz.TriggerKey;

public class CheckForExecutingTriggerResponse extends BaseNodesResponse<TransportCheckForExecutingTriggerAction.NodeResponse> {

    public CheckForExecutingTriggerResponse(StreamInput in) throws IOException {
        super(in);
    }

    public CheckForExecutingTriggerResponse(final ClusterName clusterName, List<TransportCheckForExecutingTriggerAction.NodeResponse> nodes,
            List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    public List<TransportCheckForExecutingTriggerAction.NodeResponse> readNodesFrom(final StreamInput in) throws IOException {
        return in.readList(TransportCheckForExecutingTriggerAction.NodeResponse::readNodeResponse);
    }

    @Override
    public void writeNodesTo(final StreamOutput out, List<TransportCheckForExecutingTriggerAction.NodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public String toString() {
        return "CheckForExecutingTriggerResponse [failures=" + failures() + ", nodes=" + getNodesMap() + "]";
    }

    public Set<TriggerKey> getAllRunningTriggerKeys() {
        HashSet<TriggerKey> result = new HashSet<>();

        for (TransportCheckForExecutingTriggerAction.NodeResponse nodeResponse : getNodes()) {
            for (String triggerKeyString : nodeResponse.getExecutingTriggers()) {
                result.add(parseTriggerKeyString(triggerKeyString));
            }
        }

        return result;
    }

    private TriggerKey parseTriggerKeyString(String string) {
        int p = string.indexOf('.');

        if (p == -1) {
            return new TriggerKey(string, "");
        } else {
            return new TriggerKey(string.substring(p + 1), string.substring(0, p));
        }
    }

}
