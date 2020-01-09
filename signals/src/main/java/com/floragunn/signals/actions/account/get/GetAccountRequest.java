package com.floragunn.signals.actions.account.get;

import java.io.IOException;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import com.floragunn.signals.accounts.AccountType;

public class GetAccountRequest extends ActionRequest {

    private AccountType accountType;
    private String accountId;

    public GetAccountRequest() {
        super();
    }

    public GetAccountRequest(AccountType accountType, String accountId) {
        super();
        this.accountType = accountType;
        this.accountId = accountId;
    }

    public GetAccountRequest(StreamInput in) throws IOException {
        super(in);
        this.accountId = in.readString();
        this.accountType = in.readEnum(AccountType.class);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(accountId);
        out.writeEnum(accountType);

    }

    @Override
    public ActionRequestValidationException validate() {
        if (accountId == null || accountId.length() == 0) {
            return new ActionRequestValidationException();
        }
        return null;
    }

    public AccountType getAccountType() {
        return accountType;
    }

    public void setAccountType(AccountType accountType) {
        this.accountType = accountType;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }
}
