/*
  * Copyright 2020 by floragunn GmbH - All rights reserved
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
package com.floragunn.searchguard.authtoken;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

public class AuthToken implements ToXContentObject, Writeable, Serializable {
    public static final String EXPIRES_AT = "expires_at";
    public static final String DYNAMIC_EXPIRES_AT = "dynamic_expires_at";
    public static final Map<String, Object> INDEX_MAPPING = ImmutableMap.of("dynamic", true, "properties", ImmutableMap.of("created_at",
            ImmutableMap.of("type", "date"), EXPIRES_AT, ImmutableMap.of("type", "date"), DYNAMIC_EXPIRES_AT, ImmutableMap.of("type", "date")));

    private static final long serialVersionUID = 6038589333544878668L;
    private final String userName;
    private final String tokenName;
    private final String id;
    private final Instant creationTime;
    private final Instant expiryTime;
    private final Instant revokedAt;

    private final RequestedPrivileges requestedPrivileges;
    private final AuthTokenPrivilegeBase base;

    AuthToken(String id, String userName, String tokenName, RequestedPrivileges requestedPrivileges, AuthTokenPrivilegeBase base,
            Instant creationTime, Instant expiryTime, Instant revokedAt) {
        this.id = id;
        this.userName = userName;
        this.tokenName = tokenName;
        this.requestedPrivileges = requestedPrivileges;
        this.base = base;
        this.creationTime = creationTime;
        this.expiryTime = expiryTime;
        this.revokedAt = revokedAt;
    }

    public AuthToken(StreamInput in) throws IOException {
        this.id = in.readString();
        this.userName = in.readString();
        this.tokenName = in.readOptionalString();
        this.creationTime = in.readInstant();
        this.expiryTime = in.readOptionalInstant();
        this.revokedAt = in.readOptionalInstant();

        this.requestedPrivileges = new RequestedPrivileges(in);
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
        builder.field("requested", requestedPrivileges);
        builder.field("base");
        base.toXContent(builder, params);
        builder.field("created_at", creationTime.toEpochMilli());

        if (expiryTime != null) {
            builder.field("expires_at", expiryTime.toEpochMilli());
        }

        if (revokedAt != null) {
            builder.field("revoked_at", revokedAt.toEpochMilli());
        }

        return builder;
    }

    public String getId() {
        return id;
    }

    public RequestedPrivileges getRequestedPrivileges() {
        return requestedPrivileges;
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
        AuthToken revoked = new AuthToken(id, userName, tokenName, requestedPrivileges, base, creationTime, expiryTime, Instant.now());
        revoked.getBase().setConfigSnapshot(null);
        return revoked;
    }

    public static AuthToken parse(String id, DocNode jsonNode) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors);

        String userName = vJsonNode.get("user_name").required().asString();
        String tokenName = vJsonNode.get("token_name").asString();
        AuthTokenPrivilegeBase base = null;
        RequestedPrivileges requestedPrivilges = null;

        if (vJsonNode.hasNonNull("base")) {
            try {
                base = AuthTokenPrivilegeBase.parse(jsonNode.getAsNode("base"));
            } catch (ConfigValidationException e) {
                validationErrors.add("base", e);
            }
        } else {
            validationErrors.add(new MissingAttribute("base", jsonNode));
        }

        if (vJsonNode.hasNonNull("requested")) {
            try {
                requestedPrivilges = RequestedPrivileges.parse(jsonNode.getAsNode("requested"));
            } catch (ConfigValidationException e) {
                validationErrors.add("requested", e);
            }
        } else {
            validationErrors.add(new MissingAttribute("requested", jsonNode));
        }

        Instant createdAt = vJsonNode.get("created_at").asInstantFromEpochMilli();
        Instant expiry = vJsonNode.get("expires_at").asInstantFromEpochMilli();
        Instant revokedAt = vJsonNode.get("revoked_at").asInstantFromEpochMilli();

        validationErrors.throwExceptionForPresentErrors();

        return new AuthToken(id, userName, tokenName, requestedPrivilges, base, createdAt, expiry, revokedAt);
    }

    public Instant getCreationTime() {
        return creationTime;
    }

    public Instant getExpiryTime() {
        return expiryTime;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.id);
        out.writeString(this.userName);
        out.writeOptionalString(this.tokenName);
        out.writeInstant(this.creationTime);
        out.writeOptionalInstant(this.expiryTime);
        out.writeOptionalInstant(this.revokedAt);
        this.requestedPrivileges.writeTo(out);
        this.base.writeTo(out);
    }

    public Instant getRevokedAt() {
        return revokedAt;
    }

    @Override
    public String toString() {
        return "AuthToken [userName=" + userName + ", tokenName=" + tokenName + ", id=" + id + ", creationTime=" + creationTime + ", expiryTime="
                + expiryTime + ", revokedAt=" + revokedAt + ", requestedPrivilges=" + requestedPrivileges + ", base=" + base + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((base == null) ? 0 : base.hashCode());
        result = prime * result + ((creationTime == null) ? 0 : creationTime.hashCode());
        result = prime * result + ((expiryTime == null) ? 0 : expiryTime.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((requestedPrivileges == null) ? 0 : requestedPrivileges.hashCode());
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
        if (creationTime == null) {
            if (other.creationTime != null)
                return false;
        } else if (!creationTime.equals(other.creationTime))
            return false;
        if (expiryTime == null) {
            if (other.expiryTime != null)
                return false;
        } else if (!expiryTime.equals(other.expiryTime))
            return false;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (requestedPrivileges == null) {
            if (other.requestedPrivileges != null)
                return false;
        } else if (!requestedPrivileges.equals(other.requestedPrivileges))
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
