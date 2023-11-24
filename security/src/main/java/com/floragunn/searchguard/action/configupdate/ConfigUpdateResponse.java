/*
 * Copyright 2015-2017 floragunn GmbH
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

package com.floragunn.searchguard.action.configupdate;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

public class ConfigUpdateResponse extends BaseNodesResponse<ConfigUpdateNodeResponse> implements ToXContentObject {

    public ConfigUpdateResponse(StreamInput in) throws IOException {
        super(in);
    }
    
    public ConfigUpdateResponse(final ClusterName clusterName, List<ConfigUpdateNodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    public List<ConfigUpdateNodeResponse> readNodesFrom(final StreamInput in) throws IOException {
        return in.readCollectionAsList(ConfigUpdateNodeResponse::readNodeResponse);
    }

    @Override
    public void writeNodesTo(final StreamOutput out, List<ConfigUpdateNodeResponse> nodes) throws IOException {
        out.writeCollection(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.startObject("configupdate_response");
        //builder.feld("failures", failures().get(0).);
        builder.field("nodes", getNodesMap());
        builder.field("node_size", getNodes().size());
        builder.field("has_failures", hasFailures());
        builder.field("failures_size", failures().size());
        builder.endObject();

        return builder;
    }
}
