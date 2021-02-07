/*
 * Copyright 2020 by floragunn GmbH - All rights reserved
 * 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 
 * This software is free of charge for non-commercial and academic use. 
 * For commercial use in a production environment you have to obtain a license 
 * from https://floragunn.com
 * 
 */

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
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import com.floragunn.codova.config.temporal.TemporalAmountFormat;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.searchguard.authtoken.RequestedPrivileges;

public class CreateAuthTokenRequest extends ActionRequest implements ToXContentObject {

    private String tokenName;
    private String audience;
    private TemporalAmount expiresAfter;
    private RequestedPrivileges requestedPrivileges;
    private boolean freezePrivileges = true;

    public CreateAuthTokenRequest() {
        super();
    }

    public CreateAuthTokenRequest(String tokenName, RequestedPrivileges requestedPrivileges) {
        super();
        this.tokenName = tokenName;
        this.requestedPrivileges = requestedPrivileges;

    }

    
    public CreateAuthTokenRequest(RequestedPrivileges requestedPrivileges) {
        super();
        this.tokenName = "";
        this.requestedPrivileges = requestedPrivileges;

    }

    public CreateAuthTokenRequest(StreamInput in) throws IOException {
        super(in);
        this.tokenName = in.readString();
        this.audience = in.readOptionalString();

        String expiresAfter = in.readOptionalString();

        try {
            this.expiresAfter = TemporalAmountFormat.INSTANCE.parse(expiresAfter);
        } catch (ConfigValidationException e) {
            throw new IOException("Error while parsing " + expiresAfter, e);
        }
        this.requestedPrivileges = new RequestedPrivileges(in);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(tokenName);
        out.writeOptionalString(audience);
        out.writeOptionalString(TemporalAmountFormat.INSTANCE.format(expiresAfter));
        requestedPrivileges.writeTo(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public static CreateAuthTokenRequest parse(BytesReference document, XContentType contentType) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(DocReader.format(contentType == XContentType.YAML ? Format.YAML : Format.JSON).readObject(BytesReference.toBytes(document)), validationErrors);
        CreateAuthTokenRequest result = new CreateAuthTokenRequest();

        result.tokenName = vJsonNode.get("name").asString();
        result.audience = vJsonNode.get("audience").asString();
        result.expiresAfter = vJsonNode.get("expires_after").asTemporalAmount();
        result.freezePrivileges = vJsonNode.get("freeze_privileges").withDefault(true).asBoolean();

        if (vJsonNode.hasNonNull("requested")) {
            try {
                result.requestedPrivileges = RequestedPrivileges.parse(vJsonNode.getDocumentNode().getAsNode("requested"));
            } catch (ConfigValidationException e) {
                validationErrors.add("requested", e);
            }
        } else {
            validationErrors.add(new MissingAttribute("requested", vJsonNode));
        }

        validationErrors.throwExceptionForPresentErrors();

        return result;
    }

    public RequestedPrivileges getRequestedPrivileges() {
        return requestedPrivileges;
    }

    public void setRequestedPrivileges(RequestedPrivileges requestedPrivileges) {
        this.requestedPrivileges = requestedPrivileges;
    }

    public String getAudience() {
        return audience;
    }

    public void setAudience(String audience) {
        this.audience = audience;
    }

    public TemporalAmount getExpiresAfter() {
        return expiresAfter;
    }

    public void setExpiresAfter(Duration expiresAfter) {
        this.expiresAfter = expiresAfter;
    }

    public String getTokenName() {
        return tokenName;
    }

    public void setTokenName(String tokenName) {
        this.tokenName = tokenName;
    }

    public boolean isFreezePrivileges() {
        return freezePrivileges;
    }

    public void setFreezePrivileges(boolean snapshotPrivileges) {
        this.freezePrivileges = snapshotPrivileges;
    }

    public String toJson() {
        return Strings.toString(this);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (tokenName != null) {
            builder.field("name", tokenName);
        }

        if (expiresAfter != null) {
            builder.field("expires_after", TemporalAmountFormat.INSTANCE.format(expiresAfter));
        }

        if (requestedPrivileges != null) {
            builder.field("requested", requestedPrivileges);
        }

        builder.field("freeze_privileges", freezePrivileges);

        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return "CreateAuthTokenRequest [tokenName=" + tokenName + ", audience=" + audience + ", expiresAfter=" + expiresAfter
                + ", requestedPrivileges=" + requestedPrivileges + ", freezePrivileges=" + freezePrivileges + "]";
    }

}
