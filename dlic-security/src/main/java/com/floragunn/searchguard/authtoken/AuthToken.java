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
    private final Long revokedAt;

    private final RequestedPrivileges requestedPrivilges;
    private final AuthTokenPrivilegeBase base;

    AuthToken(String id, String userName, String tokenName, RequestedPrivileges requestedPrivilges, AuthTokenPrivilegeBase base, long creationTime,
            long expiryTime, Long revokedAt) {
        this.id = id;
        this.userName = userName;
        this.tokenName = tokenName;
        this.requestedPrivilges = requestedPrivilges;
        this.base = base;
        this.creationTime = creationTime;
        this.expiryTime = expiryTime;
        this.revokedAt = revokedAt;
    }

    public AuthToken(StreamInput in) throws IOException {
        this.id = in.readString();
        this.userName = in.readString();
        this.tokenName = in.readOptionalString();
        this.creationTime = in.readLong();
        this.expiryTime = in.readLong();
        this.revokedAt = in.readOptionalLong();

        this.requestedPrivilges = new RequestedPrivileges(in);
        this.base = new AuthTokenPrivilegeBase(in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toXContentFragment(builder, params);
        builder.endObject();
        return builder;
    }

    public XContentBuilder toXContentFragment(XContentBuilder builder, Params params) throws IOException {
        builder.field("user_name", userName);
        builder.field("token_name", tokenName);
        builder.field("requested", requestedPrivilges);
        builder.field("base");
        base.toXContent(builder, params);
        builder.field("created_at", creationTime);
        builder.field("expires_at", expiryTime);

        if (revokedAt != null) {
            builder.field("revoked_at", revokedAt);
        }

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

    public boolean isRevoked() {
        return revokedAt != null;
    }
    
    AuthToken getRevokedInstance() {
        AuthToken revoked = new AuthToken(id, userName, tokenName, requestedPrivilges, base, creationTime, expiryTime, System.currentTimeMillis());
        revoked.getBase().setConfigSnapshot(null);
        return revoked;
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
        Long revokedAt = vJsonNode.longNumber("revoked_at", null);

        validationErrors.throwExceptionForPresentErrors();

        return new AuthToken(id, userName, tokenName, requestedPrivilges, base, createdAt, expiry, revokedAt);
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
        out.writeOptionalLong(this.revokedAt);
        this.requestedPrivilges.writeTo(out);
        this.base.writeTo(out);
    }

    public Long getRevokedAt() {
        return revokedAt;
    }

    @Override
    public String toString() {
        return "AuthToken [userName=" + userName + ", tokenName=" + tokenName + ", id=" + id + ", creationTime=" + creationTime + ", expiryTime="
                + expiryTime + ", revokedAt=" + revokedAt + ", requestedPrivilges=" + requestedPrivilges + ", base=" + base + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((base == null) ? 0 : base.hashCode());
        result = prime * result + (int) (creationTime ^ (creationTime >>> 32));
        result = prime * result + (int) (expiryTime ^ (expiryTime >>> 32));
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((requestedPrivilges == null) ? 0 : requestedPrivilges.hashCode());
        result = prime * result + ((revokedAt == null) ? 0 : revokedAt.hashCode());
        result = prime * result + ((tokenName == null) ? 0 : tokenName.hashCode());
        result = prime * result + ((userName == null) ? 0 : userName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        AuthToken other = (AuthToken) obj;
        if (base == null) {
            if (other.base != null)
                return false;
        } else if (!base.equals(other.base))
            return false;
        if (creationTime != other.creationTime)
            return false;
        if (expiryTime != other.expiryTime)
            return false;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (requestedPrivilges == null) {
            if (other.requestedPrivilges != null)
                return false;
        } else if (!requestedPrivilges.equals(other.requestedPrivilges))
            return false;
        if (revokedAt == null) {
            if (other.revokedAt != null)
                return false;
        } else if (!revokedAt.equals(other.revokedAt))
            return false;
        if (tokenName == null) {
            if (other.tokenName != null)
                return false;
        } else if (!tokenName.equals(other.tokenName))
            return false;
        if (userName == null) {
            if (other.userName != null)
                return false;
        } else if (!userName.equals(other.userName))
            return false;
        return true;
    }

}
