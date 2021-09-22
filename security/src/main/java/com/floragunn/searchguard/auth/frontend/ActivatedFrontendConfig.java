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

package com.floragunn.searchguard.auth.frontend;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.floragunn.codova.documents.Document;

public class ActivatedFrontendConfig {
    private List<AuthMethod> authMethods;

    private ActivatedFrontendConfig(List<AuthMethod> authMethods) {
        this.authMethods = authMethods;
    }

    public List<AuthMethod> getAuthMethods() {
        return authMethods;
    }

    public static class AuthMethod implements ToXContentObject, Writeable, Document {

        private final String id;
        private final String method;
        private final boolean session;
        private final boolean unavailable;
        private final String label;
        private final String message;
        private final String ssoLocation;
        private final String ssoContext;
        private final Map<String, Object> config;

        public AuthMethod(String method, String label, String id) {
            this.method = method;
            this.label = label;
            this.id = id;
            this.session = true;
            this.unavailable = false;
            this.message = null;
            this.config = Collections.emptyMap();
            this.ssoLocation = null;
            this.ssoContext = null;
        }

        public AuthMethod(String method, String label, String id, boolean session, boolean unavailable, String message) {
            this.method = method;
            this.label = label;
            this.id = id;
            this.session = session;
            this.unavailable = unavailable;
            this.message = message;
            this.config = Collections.emptyMap();
            this.ssoLocation = null;
            this.ssoContext = null;
        }

        private AuthMethod(String method, String label, String id, boolean session, boolean unavailable, String message, String ssoLocation,
                String ssoContext, Map<String, Object> config) {
            this.method = method;
            this.id = id;
            this.session = session;
            this.unavailable = unavailable;
            this.label = label;
            this.message = message;
            this.ssoLocation = ssoLocation;
            this.ssoContext = ssoContext;
            this.config = Collections.unmodifiableMap(new HashMap<>(config));
        }

        public AuthMethod(StreamInput in) throws IOException {
            this.method = in.readString();
            this.id = in.readOptionalString();
            this.session = in.readBoolean();
            this.label = in.readString();
            this.message = in.readOptionalString();
            this.unavailable = in.readBoolean();
            this.ssoLocation = in.readOptionalString();
            this.ssoContext = in.readOptionalString();
            this.config = in.readMap();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("method", method);

            if (id != null) {
                builder.field("id", id);
            }

            builder.field("session", session);
            builder.field("label", label);

            if (unavailable) {
                builder.field("unavailable", unavailable);
            }

            if (message != null) {
                builder.field("message", message);
            }

            if (ssoLocation != null) {
                builder.field("sso_location", ssoLocation);
            }

            if (ssoContext != null) {
                builder.field("sso_context", ssoContext);
            }

            if (config.size() > 0) {
                builder.field("config", config);
            }

            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(method);
            out.writeOptionalString(id);
            out.writeBoolean(session);
            out.writeString(label);
            out.writeOptionalString(message);
            out.writeBoolean(unavailable);
            out.writeOptionalString(ssoLocation);
            out.writeOptionalString(ssoContext);
            out.writeMap(config);
        }

        public AuthMethod unavailable(String message) {
            return new AuthMethod(method, label, id, session, true, message, ssoLocation, ssoContext, config);
        }

        public AuthMethod unavailableDueToConfigurationError() {
            return unavailable("Unavailable due to configuration error. Please contact your system administrator");
        }

        public AuthMethod temporarilyUnavailable() {
            return unavailable("Temporarily unavailable due to configuration error. Please try again later or contact your system administrator");
        }

        public AuthMethod ssoLocation(String ssoLocation) {
            return new AuthMethod(method, label, id, session, unavailable, message, ssoLocation, ssoContext, config);
        }

        public AuthMethod ssoContext(String ssoContext) {
            return new AuthMethod(method, label, id, session, unavailable, message, ssoLocation, ssoContext, config);
        }

        public AuthMethod config(Map<String, Object> config) {
            return new AuthMethod(method, label, id, session, unavailable, message, ssoLocation, ssoContext, config);
        }

        public AuthMethod config(String key, Object value) {
            HashMap<String, Object> newConfig = new HashMap<>(this.config);
            newConfig.put(key, value);
            return new AuthMethod(method, label, id, session, unavailable, message, ssoLocation, ssoContext, newConfig);
        }

        public String getMethod() {
            return method;
        }

        public boolean isSession() {
            return session;
        }

        public String getLabel() {
            return label;
        }

        public String getSsoLocation() {
            return ssoLocation;
        }

        public String getSsoContext() {
            return ssoContext;
        }

        public AuthMethod clone() {
            return new AuthMethod(method, label, id, session, unavailable, message, ssoLocation, ssoContext, config);
        }

        public Map<String, Object> getConfig() {
            return config;
        }

        public String getId() {
            return id;
        }

        @Override
        public Map<String, Object> toMap() {
            Map<String, Object> result = new LinkedHashMap<>();
                        
            result.put("method", method);

            if (id != null) {
                result.put("id", id);
            }

            result.put("session", session);
            result.put("label", label);

            if (unavailable) {
                result.put("unavailable", unavailable);
            }

            if (message != null) {
                result.put("message", message);
            }

            if (ssoLocation != null) {
                result.put("sso_location", ssoLocation);
            }

            if (ssoContext != null) {
                result.put("sso_context", ssoContext);
            }

            if (config.size() > 0) {
                result.put("config", config);
            }

            return result;
        }
        
        @Override
        public String toString() {
            return toJsonString();
        }

    }

}
