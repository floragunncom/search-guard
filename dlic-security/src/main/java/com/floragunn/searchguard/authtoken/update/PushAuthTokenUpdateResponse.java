/*
 * Copyright 2020 by floragunn GmbH - All rights reserved
 * 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 
 * This software is free of charge for non-commercial and academic use. 
 * For commercial use in a production environment you have to obtain a license 
 * from https://floragunn.com
 * 
 */

package com.floragunn.searchguard.authtoken.update;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class PushAuthTokenUpdateResponse extends BaseNodesResponse<PushAuthTokenUpdateNodeResponse> {

    public PushAuthTokenUpdateResponse(StreamInput in) throws IOException {
        super(in);
    }

    public PushAuthTokenUpdateResponse(final ClusterName clusterName, List<PushAuthTokenUpdateNodeResponse> nodes,
            List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    public List<PushAuthTokenUpdateNodeResponse> readNodesFrom(final StreamInput in) throws IOException {
        return in.readCollectionAsList(PushAuthTokenUpdateNodeResponse::readNodeResponse);
    }

    @Override
    public void writeNodesTo(final StreamOutput out, List<PushAuthTokenUpdateNodeResponse> nodes) throws IOException {
        out.writeCollection(nodes);
    }

    @Override
    public String toString() {
        return "PushAuthTokenUpdateResponse [failures()=" + failures() + ", getNodes()=" + getNodes() + "]";
    }
}
