/*
 * Copyright 2023 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.floragunn.signals.actions.account.put;

import java.io.IOException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentType;

public class PutAccountRequest extends ActionRequest {

    private String accountType;
    private String accountId;
    private BytesReference body;
    private XContentType bodyContentType;

    public PutAccountRequest() {
        super();
    }

    public PutAccountRequest(String accountType, String accountId, BytesReference body, XContentType bodyContentType) {
        super();
        this.accountType = accountType;
        this.accountId = accountId;
        this.body = body;
        this.bodyContentType = bodyContentType;
    }

    public PutAccountRequest(StreamInput in) throws IOException {
        super(in);
        this.accountId = in.readString();
        this.accountType = in.readString();
        this.body = in.readBytesReference();
        this.bodyContentType = in.readEnum(XContentType.class);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(accountId);
        out.writeString(accountType);
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
