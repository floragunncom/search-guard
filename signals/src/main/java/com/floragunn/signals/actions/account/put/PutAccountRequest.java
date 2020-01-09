package com.floragunn.signals.actions.account.put;

import java.io.IOException;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentType;

import com.floragunn.signals.accounts.AccountType;

public class PutAccountRequest extends ActionRequest {

    private AccountType accountType;
    private String accountId;
    private BytesReference body;
    private XContentType bodyContentType;

    public PutAccountRequest() {
        super();
    }

    public PutAccountRequest(AccountType accountType, String accountId, BytesReference body, XContentType bodyContentType) {
        super();
        this.accountType = accountType;
        this.accountId = accountId;
        this.body = body;
        this.bodyContentType = bodyContentType;
    }

    public PutAccountRequest(StreamInput in) throws IOException {
        super(in);
        this.accountId = in.readString();
        this.accountType = in.readEnum(AccountType.class);
        this.body = in.readBytesReference();
        this.bodyContentType = in.readEnum(XContentType.class);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(accountId);
        out.writeEnum(accountType);
        out.writeBytesReference(body);
        out.writeEnum(bodyContentType);
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

    public BytesReference getBody() {
        return body;
    }

    public void setBody(BytesReference body) {
        this.body = body;
    }

    public XContentType getBodyContentType() {
        return bodyContentType;
    }

    public void setBodyContentType(XContentType bodyContentType) {
        this.bodyContentType = bodyContentType;
    }

}
