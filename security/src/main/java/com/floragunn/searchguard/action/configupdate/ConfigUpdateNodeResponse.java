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
import java.util.Arrays;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

public class ConfigUpdateNodeResponse extends BaseNodeResponse implements ToXContentObject {
    
    private String[] updatedConfigTypes;
    private String message;
    
    public ConfigUpdateNodeResponse(StreamInput in) throws IOException {
        super(in);
        updatedConfigTypes = in.readStringArray();
        message = in.readOptionalString();
    }

    public ConfigUpdateNodeResponse(final DiscoveryNode node, String[] updatedConfigTypes, String message) {
        super(node);
        this.updatedConfigTypes = updatedConfigTypes;
        this.message = message;
    }
    
    public static ConfigUpdateNodeResponse readNodeResponse(StreamInput in) throws IOException {
        return new ConfigUpdateNodeResponse(in);
    }
    
    public String[] getUpdatedConfigTypes() {
        return updatedConfigTypes==null?null:Arrays.copyOf(updatedConfigTypes, updatedConfigTypes.length);
    }

    public String getMessage() {
        return message;
    }
    
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(updatedConfigTypes);
        out.writeOptionalString(message);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("updated_config_types", updatedConfigTypes);
        builder.field("updated_config_size", updatedConfigTypes == null?0:updatedConfigTypes.length);
        builder.field("message", message);
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return "ConfigUpdateNodeResponse [updatedConfigTypes=" + Arrays.toString(updatedConfigTypes) + ", message=" + message + "]";
    }
}
