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

package com.floragunn.searchguard.authtoken.api;

import java.io.IOException;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

public class RevokeAuthTokenResponse extends ActionResponse implements ToXContentObject {

    private String info;
    private RestStatus restStatus;
    private String error;

    public RevokeAuthTokenResponse(String status) {
        this.info = status;
        this.restStatus = RestStatus.OK;
    }

    public RevokeAuthTokenResponse(RestStatus restStatus, String error) {
        this.restStatus = restStatus;
        this.error = error;
    }

    public RevokeAuthTokenResponse(StreamInput in) throws IOException {
        this.info = in.readOptionalString();
        this.restStatus = in.readEnum(RestStatus.class);
        this.error = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(info);
        out.writeEnum(this.restStatus);
        out.writeOptionalString(this.error);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (restStatus != null) {
            builder.field("status", restStatus.getStatus());
        }

        if (info != null) {
            builder.field("info", info);
        }

        if (error != null) {
            builder.field("error", error);
        }

        builder.endObject();
        return builder;
    }

    public RestStatus status() {
        return restStatus;
    }

}
