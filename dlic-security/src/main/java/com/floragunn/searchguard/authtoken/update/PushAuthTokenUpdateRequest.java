package com.floragunn.searchguard.authtoken.update;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import com.floragunn.searchguard.authtoken.AuthToken;

public class PushAuthTokenUpdateRequest extends BaseNodesRequest<PushAuthTokenUpdateRequest> {

    private List<AuthToken> updatedTokens;

    public PushAuthTokenUpdateRequest(StreamInput in) throws IOException {
        super(in);
        this.updatedTokens = in.readList(AuthToken::new);
    }

    public PushAuthTokenUpdateRequest(List<AuthToken> updatedTokens) {
        super(new String[0]);
        this.updatedTokens = updatedTokens;
    }

    public PushAuthTokenUpdateRequest(AuthToken updatedToken) {
        this(Collections.singletonList(updatedToken));
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(updatedTokens);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public List<AuthToken> getUpdatedTokens() {
        return updatedTokens;
    }

    public void setUpdatedTokens(List<AuthToken> updatedTokens) {
        this.updatedTokens = updatedTokens;
    }

    @Override
    public String toString() {
        return "PushAuthTokenUpdateRequest [updatedTokens=" + updatedTokens + "]";
    }
}
