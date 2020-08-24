package com.floragunn.searchguard.authtoken.api;

import java.io.IOException;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

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
        super(in);
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
