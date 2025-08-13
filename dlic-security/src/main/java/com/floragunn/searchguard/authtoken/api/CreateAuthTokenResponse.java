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
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import com.floragunn.searchguard.authtoken.AuthToken;

public class CreateAuthTokenResponse extends ActionResponse implements ToXContentObject {

    private AuthToken authToken;
    private String jwt;

    public CreateAuthTokenResponse() {
    }

    public CreateAuthTokenResponse(AuthToken authToken, String jwt) {
        this.authToken = authToken;
        this.jwt = jwt;
    }

    public CreateAuthTokenResponse(StreamInput in) throws IOException {
        this.authToken = new AuthToken(in);
        this.jwt = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.authToken.writeTo(out);
        out.writeString(jwt);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("token", jwt);
        builder.field("id", authToken.getId());
        authToken.toXContentFragment(builder, params);
        builder.endObject();
        return builder;
    }

    public AuthToken getAuthToken() {
        return authToken;
    }

    public String getJwt() {
        return jwt;
    }

}
