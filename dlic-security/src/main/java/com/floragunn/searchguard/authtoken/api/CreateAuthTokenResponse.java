package com.floragunn.searchguard.authtoken.api;

import java.io.IOException;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class CreateAuthTokenResponse extends ActionResponse implements ToXContentObject {

    private String token;

    public CreateAuthTokenResponse() {
    }

    public CreateAuthTokenResponse(String token) {
        this.token = token;
    }

    public CreateAuthTokenResponse(StreamInput in) throws IOException {
        super(in);
        this.token = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(token);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("token", token);
        builder.endObject();
        return builder;
    }

}
