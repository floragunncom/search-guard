package com.floragunn.signals.actions.account.delete;

import java.io.IOException;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class DeleteAccountRequest extends ActionRequest {

    private String accountType;
    private String accountId;

    public DeleteAccountRequest(String accountType, String accountId) {
        super();
        this.accountType = accountType;
        this.accountId = accountId;
    }

    public DeleteAccountRequest(StreamInput in) throws IOException {
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
