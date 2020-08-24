package com.floragunn.searchguard.authtoken.api;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.TemporalAmount;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import com.floragunn.searchguard.authtoken.RequestedPrivileges;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.config.validation.MissingAttribute;
import com.floragunn.searchsupport.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.config.validation.ValidatingJsonParser;
import com.floragunn.searchsupport.config.validation.ValidationErrors;
import com.floragunn.searchsupport.util.temporal.TemporalAmountFormat;

public class RevokeAuthTokenRequest extends ActionRequest {

    private String authTokenId;

    public RevokeAuthTokenRequest() {
        super();
    }

    public RevokeAuthTokenRequest(String authTokenId) {
        super();
        this.authTokenId = authTokenId;

    }

    public RevokeAuthTokenRequest(StreamInput in) throws IOException {
        super(in);
        this.authTokenId = in.readString();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(authTokenId);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String getAuthTokenId() {
        return authTokenId;
    }

}
