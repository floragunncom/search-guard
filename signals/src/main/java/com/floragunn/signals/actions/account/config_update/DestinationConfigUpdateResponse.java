/*
 * Copyright 2023 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
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
