package com.floragunn.searchguard.authtoken;

import java.io.IOException;
import java.io.Serializable;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.config.validation.MissingAttribute;
import com.floragunn.searchsupport.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.config.validation.ValidationErrors;

public class AuthToken implements ToXContentObject, Writeable, Serializable {
    private static final long serialVersionUID = 6038589333544878668L;
    private final String userName;
    private final String tokenName;
    private final String id;
    private final long creationTime;
    private final long expiryTime;

    private final RequestedPrivileges requestedPrivilges;
    private final AuthTokenPrivilegeBase base;

    AuthToken(String id, String userName, String tokenName, RequestedPrivileges requestedPrivilges, AuthTokenPrivilegeBase base, long creationTime,
            long expiryTime) {
        this.id = id;
        this.userName = userName;
        this.tokenName = tokenName;
        this.requestedPrivilges = requestedPrivilges;
        this.base = base;
        this.creationTime = creationTime;
        this.expiryTime = expiryTime;
    }

    public AuthToken(StreamInput in) throws IOException {
        this.id = in.readString();
        this.userName = in.readString();
        this.tokenName = in.readOptionalString();
        this.creationTime = in.readLong();
        this.expiryTime = in.readLong();

        this.requestedPrivilges = new RequestedPrivileges(in);
        this.base = new AuthTokenPrivilegeBase(in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("user_name", userName);
        builder.field("token_name", tokenName);
        builder.field("requested", requestedPrivilges);
        builder.field("base", base);
        builder.field("created_at", creationTime);
        builder.field("expires_at", expiryTime);

        builder.endObject();
        return builder;
    }

    public String getId() {
        return id;
    }

    public RequestedPrivileges getRequestedPrivilges() {
        return requestedPrivilges;
    }

    public String getUserName() {
        return userName;
    }

    public String getTokenName() {
        return tokenName;
    }

    public AuthTokenPrivilegeBase getBase() {
        return base;
    }

    public static AuthToken parse(String id, JsonNode jsonNode) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode, validationErrors);

        String userName = vJsonNode.requiredString("user_name");
        String tokenName = vJsonNode.string("token_name");
        AuthTokenPrivilegeBase base = null;
        RequestedPrivileges requestedPrivilges = null;

        if (vJsonNode.hasNonNull("base")) {
            try {
                base = AuthTokenPrivilegeBase.parse(vJsonNode.get("base"));
            } catch (ConfigValidationException e) {
                validationErrors.add("base", e);
            }
        } else {
            validationErrors.add(new MissingAttribute("base", jsonNode));
        }

        if (vJsonNode.hasNonNull("requested")) {
            try {
                requestedPrivilges = RequestedPrivileges.parse(vJsonNode.get("requested"));
            } catch (ConfigValidationException e) {
                validationErrors.add("requested", e);
            }
        } else {
            validationErrors.add(new MissingAttribute("requested", jsonNode));
        }

        long createdAt = vJsonNode.requiredLong("created_at");
        long expiry = vJsonNode.requiredLong("expires_at");

        validationErrors.throwExceptionForPresentErrors();

        return new AuthToken(id, userName, tokenName, requestedPrivilges, base, createdAt, expiry);
    }

    @Override
    public String toString() {
        return "AuthToken [userName=" + userName + ", tokenName=" + tokenName + ", id=" + id + ", requestedPrivilges=" + requestedPrivilges
                + ", base=" + base + "]";
    }

    public long getCreationTime() {
        return creationTime;
    }

    public long getExpiryTime() {
        return expiryTime;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.id);
        out.writeString(this.userName);
        out.writeOptionalString(this.tokenName);
        out.writeLong(this.creationTime);
        out.writeLong(this.expiryTime);
        this.requestedPrivilges.writeTo(out);
        this.base.writeTo(out);
    }

}
