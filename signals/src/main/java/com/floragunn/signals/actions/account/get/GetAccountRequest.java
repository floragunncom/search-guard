package com.floragunn.signals.actions.account.get;

import java.io.IOException;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

public class GetAccountRequest extends ActionRequest {

    private String accountType;
    private String accountId;

    public GetAccountRequest() {
        super();
    }

    public GetAccountRequest(String accountType, String accountId) {
        super();
        this.accountType = accountType;
        this.accountId = accountId;
    }

    public GetAccountRequest(StreamInput in) throws IOException {
        super(in);
        this.accountId = in.readString();
        this.accountType = in.readString();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(accountId);
        out.writeString(accountType);

    }

    @Override
    public ActionRequestValidationException validate() {
        if (accountId == null || accountId.length() == 0) {
            return new ActionRequestValidationException();
        }
        return null;
    }

    public String getAccountType() {
        return accountType;
    }

    public void setAccountType(String accountType) {
        this.accountType = accountType;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }
}
