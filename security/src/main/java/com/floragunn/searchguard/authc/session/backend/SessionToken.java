/*
 * Copyright 2021 floragunn GmbH
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

package com.floragunn.searchguard.authc.session.backend;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.Map;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.google.common.collect.ImmutableMap;

public class SessionToken implements ToXContentObject, Writeable, Serializable {
    private static final long serialVersionUID = -968321214418424644L;
    public static final String EXPIRES_AT = "expires_at";
    public static final String DYNAMIC_EXPIRES_AT = "dynamic_expires_at";
    public static final Map<String, Object> INDEX_MAPPING = ImmutableMap.of("dynamic", true, "properties", ImmutableMap.of("created_at",
            ImmutableMap.of("type", "date"), EXPIRES_AT, ImmutableMap.of("type", "date"), DYNAMIC_EXPIRES_AT, ImmutableMap.of("type", "date")));

    private final String userName;
    private final String tokenName;
    private final String id;
    private final Instant creationTime;
    private final Instant expiryTime;
    private final Instant revokedAt;
    private final Instant dynamicExpiryTime;

    private final SessionPrivileges base;

    SessionToken(String id, String userName, SessionPrivileges base, Instant creationTime, Instant expiryTime,
            Instant dynamicExpiryTime, Instant revokedAt) {
        this.id = id;
        this.userName = userName;
        this.tokenName = null;
        this.base = base;
        this.creationTime = creationTime;
        this.expiryTime = expiryTime;
        this.dynamicExpiryTime = dynamicExpiryTime;
        this.revokedAt = revokedAt;
    }

    public SessionToken(StreamInput in) throws IOException {
        this.id = in.readString();
        this.userName = in.readString();
        this.tokenName = in.readOptionalString();
        this.creationTime = in.readInstant();
        this.expiryTime = in.readOptionalInstant();
        this.dynamicExpiryTime = in.readOptionalInstant();
        this.revokedAt = in.readOptionalInstant();
        this.base = new SessionPrivileges(in);
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
        builder.field("base");
        base.toXContent(builder, params);
        builder.field("created_at", creationTime.toEpochMilli());

        if (expiryTime != null) {
            builder.field("expires_at", expiryTime.toEpochMilli());
        }

        if (dynamicExpiryTime != null) {
            builder.field(DYNAMIC_EXPIRES_AT, dynamicExpiryTime.toEpochMilli());
        }

        if (revokedAt != null) {
            builder.field("revoked_at", revokedAt.toEpochMilli());
        }

        return builder;
    }

    public String getId() {
        return id;
    }

    public String getUserName() {
        return userName;
    }

    public String getTokenName() {
        return tokenName;
    }

    public SessionPrivileges getBase() {
        return base;
    }

    public boolean isRevoked() {
        return revokedAt != null;
    }

    SessionToken getRevokedInstance() {
        SessionToken revoked = new SessionToken(id, userName, base, creationTime, expiryTime, dynamicExpiryTime, Instant.now());
        return revoked;
    }

    public static SessionToken parse(String id, DocNode jsonNode) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors);

        String userName = vJsonNode.get("user_name").required().asString();
        SessionPrivileges base = null;

        if (vJsonNode.hasNonNull("base")) {
            try {
                base = SessionPrivileges.parse(jsonNode.getAsNode("base"));
            } catch (ConfigValidationException e) {
                validationErrors.add("base", e);
            }
        } else {
            validationErrors.add(new MissingAttribute("base", jsonNode));
        }

        Instant createdAt = vJsonNode.get("created_at").asInstantFromEpochMilli();
        Instant expiry = vJsonNode.get("expires_at").asInstantFromEpochMilli();
        Instant dynamicExpiry = vJsonNode.get(SessionToken.DYNAMIC_EXPIRES_AT).asInstantFromEpochMilli();
        Instant revokedAt = vJsonNode.get("revoked_at").asInstantFromEpochMilli();

        validationErrors.throwExceptionForPresentErrors();

        return new SessionToken(id, userName,  base, createdAt, expiry, dynamicExpiry, revokedAt);
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
        out.writeOptionalInstant(this.dynamicExpiryTime);
        out.writeOptionalInstant(this.revokedAt);
        this.base.writeTo(out);
    }

    public Instant getRevokedAt() {
        return revokedAt;
    }

    @Override
    public String toString() {
        return "AuthToken [userName=" + userName + ", tokenName=" + tokenName + ", id=" + id + ", creationTime=" + creationTime + ", expiryTime="
                + expiryTime + ", revokedAt=" + revokedAt + ", base=" + base + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((base == null) ? 0 : base.hashCode());
        result = prime * result + ((creationTime == null) ? 0 : creationTime.hashCode());
        result = prime * result + ((expiryTime == null) ? 0 : expiryTime.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
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
        SessionToken other = (SessionToken) obj;
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

    public Instant getDynamicExpiryTime() {
        return dynamicExpiryTime;
    }

}
