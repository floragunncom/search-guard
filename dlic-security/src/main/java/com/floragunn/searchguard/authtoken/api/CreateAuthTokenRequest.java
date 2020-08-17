package com.floragunn.searchguard.authtoken.api;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentType;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchguard.authtoken.RequestedPrivileges;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.config.validation.MissingAttribute;
import com.floragunn.searchsupport.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.config.validation.ValidatingJsonParser;
import com.floragunn.searchsupport.config.validation.ValidationError;
import com.floragunn.searchsupport.config.validation.ValidationErrors;

public class CreateAuthTokenRequest extends ActionRequest {

    private String audience;
    private Duration expiresAfter;
    private RequestedPrivileges requestedPrivileges;

    public CreateAuthTokenRequest() {
        super();
    }

    public CreateAuthTokenRequest(RequestedPrivileges requestedPrivileges) {
        super();
        this.requestedPrivileges = requestedPrivileges;

    }

    public CreateAuthTokenRequest(StreamInput in) throws IOException {
        super(in);
        this.audience = in.readOptionalString();

        Long expiresAfterMillis = in.readOptionalLong();

        this.expiresAfter = expiresAfterMillis != null ? Duration.ofMillis(expiresAfterMillis) : null;
        this.requestedPrivileges = new RequestedPrivileges(in);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(audience);
        out.writeOptionalLong(expiresAfter != null ? expiresAfter.toMillis() : null);
        requestedPrivileges.writeTo(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public static CreateAuthTokenRequest parse(BytesReference document, XContentType contentType) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(ValidatingJsonParser.readTree(document, contentType), validationErrors);
        CreateAuthTokenRequest result = new CreateAuthTokenRequest();

        result.audience = vJsonNode.string("audience");
        result.expiresAfter = vJsonNode.duration("expires_after");

        if (vJsonNode.hasNonNull("requested")) {
            try {
                result.requestedPrivileges = RequestedPrivileges.parse(vJsonNode.get("requested"));
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

    public Duration getExpiresAfter() {
        return expiresAfter;
    }

    public void setExpiresAfter(Duration expiresAfter) {
        this.expiresAfter = expiresAfter;
    }

}
