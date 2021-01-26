/*
 * Copyright 2021 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.authtoken.api;

import java.io.IOException;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;

public class AuthTokenInfoResponse extends ActionResponse implements StatusToXContentObject {

    private boolean enabled;
    private boolean initialized;

    public AuthTokenInfoResponse() {
    }

    public AuthTokenInfoResponse(boolean enabled, boolean initialized) {
        this.enabled = enabled;
        this.initialized = initialized;
    }

    public AuthTokenInfoResponse(StreamInput in) throws IOException {
        super(in);
        in.readInt();
        this.enabled = in.readBoolean();
        this.initialized = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(1);
        out.writeBoolean(enabled);
        out.writeBoolean(initialized);

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.startObject();

        builder.field("enabled", enabled);
        builder.field("initialized", initialized);

        builder.endObject();

        return builder;
    }

    @Override
    public RestStatus status() {
        return RestStatus.OK;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public void setInitialized(boolean initialized) {
        this.initialized = initialized;
    }

}
