package com.floragunn.searchguard.authtoken.api;

import java.io.IOException;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class RevokeAuthTokenResponse extends ActionResponse implements ToXContentObject {

    private String status;

    public RevokeAuthTokenResponse(String status) {
        this.status = status;
    }

    public RevokeAuthTokenResponse(StreamInput in) throws IOException {
        super(in);
        this.status = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(status);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("status", status);
        builder.endObject();
        return builder;
    }

    public String getStatus() {
        return status;
    }

}
