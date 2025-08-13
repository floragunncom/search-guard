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

import com.floragunn.searchguard.authtoken.AuthToken;

public class GetAuthTokenResponse extends ActionResponse implements ToXContentObject {

    private AuthToken authToken;
    private RestStatus restStatus;
    private String error;

    public GetAuthTokenResponse() {
    }

    public GetAuthTokenResponse(AuthToken authToken) {
        this.authToken = authToken;
        this.restStatus = RestStatus.OK;
    }

    public GetAuthTokenResponse(RestStatus restStatus, String error) {
        this.restStatus = restStatus;
        this.error = error;
    }

    public GetAuthTokenResponse(StreamInput in) throws IOException {
        this.restStatus = in.readEnum(RestStatus.class);
        this.error = in.readOptionalString();

        if (in.readBoolean()) {
            this.authToken = new AuthToken(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this.restStatus);
        out.writeOptionalString(this.error);
        out.writeBoolean(this.authToken != null);

        if (this.authToken != null) {
            this.authToken.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (authToken != null) {
            authToken.toXContent(builder, params);
        } else {
            builder.startObject();

            if (restStatus != null) {
                builder.field("status", restStatus.getStatus());
            }

            if (error != null) {
                builder.field("error", error);
            }

            builder.endObject();
        }

        return builder;
    }

    public AuthToken getAuthToken() {
        return authToken;
    }

    public RestStatus status() {
        return restStatus;
    }

}
