/*
 * Copyright 2015-2022 floragunn GmbH
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
package com.floragunn.searchguard.authc.base;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.rest.RestStatus;

import com.floragunn.codova.documents.DocWriter;
import com.floragunn.codova.documents.Document;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.util.ImmutableMap;

public class AuthczResult implements ToXContentObject, Document<AuthczResult> {
    public static final AuthczResult STOP = new AuthczResult(Status.STOP);
    public static final AuthczResult PASS_ANONYMOUS = new AuthczResult(User.ANONYMOUS, Status.PASS);
    public static final AuthczResult PASS_WITHOUT_AUTH = new AuthczResult(null, Status.PASS);

    public static AuthczResult stop(RestStatus restStatus, String message) {
        return new AuthczResult(Status.STOP, restStatus, message);
    }

    public static AuthczResult stop(RestStatus restStatus, String message, List<DebugInfo> debug) {
        return new AuthczResult(Status.STOP, restStatus, message, null, ImmutableMap.empty(), debug);
    }

    public static AuthczResult stop(RestStatus restStatus, String message, ImmutableMap<String, String> headers, List<DebugInfo> debug) {
        return new AuthczResult(Status.STOP, restStatus, message, null, headers, debug);
    }

    public static AuthczResult pass(User user) {
        return new AuthczResult(user, Status.PASS);
    }

    public static AuthczResult pass(User user, String redirectUri) {
        return new AuthczResult(user, Status.PASS, redirectUri);
    }
    
    public static AuthczResult pass(User user, String redirectUri, List<DebugInfo> debug) {
        return new AuthczResult(user, Status.PASS, redirectUri, debug);
    }

    private final User user;
    private final Status status;
    private final RestStatus restStatus;
    private final String restStatusMessage;
    private final String redirectUri;
    private final List<DebugInfo> debug;
    private final Map<String, String> headers;

    public AuthczResult(User user, Status status) {
        this.user = user;
        this.status = status;
        this.restStatus = null;
        this.restStatusMessage = null;
        this.debug = null;
        this.redirectUri = null;
        this.headers = ImmutableMap.empty();
    }

    public AuthczResult(User user, Status status, String redirectUri) {
        this.user = user;
        this.status = status;
        this.restStatus = null;
        this.restStatusMessage = null;
        this.debug = null;
        this.redirectUri = redirectUri;
        this.headers = ImmutableMap.empty();
    }
    
    public AuthczResult(User user, Status status, String redirectUri, List<DebugInfo> debug) {
        this.user = user;
        this.status = status;
        this.restStatus = null;
        this.restStatusMessage = null;
        this.debug = debug;
        this.redirectUri = redirectUri;
        this.headers = ImmutableMap.empty();
    }

    public AuthczResult(Status status) {
        this.user = null;
        this.status = status;
        this.restStatus = null;
        this.restStatusMessage = null;
        this.debug = null;
        this.redirectUri = null;
        this.headers = ImmutableMap.empty();
    }

    public AuthczResult(Status status, RestStatus restStatus, String restStatusMessage) {
        this.user = null;
        this.status = status;
        this.restStatus = restStatus;
        this.restStatusMessage = restStatusMessage;
        this.debug = null;
        this.redirectUri = null;
        this.headers = ImmutableMap.empty();
    }

    public AuthczResult(Status status, RestStatus restStatus, String restStatusMessage, String redirectUri, ImmutableMap<String, String> headers,
            List<DebugInfo> debug) {
        this.user = null;
        this.status = status;
        this.restStatus = restStatus;
        this.restStatusMessage = restStatusMessage;
        this.debug = debug != null ? Collections.unmodifiableList(new ArrayList<>(debug)) : null;
        this.redirectUri = redirectUri;
        this.headers = headers;
    }

    public static enum Status {
        PASS, STOP
    }

    public User getUser() {
        return user;
    }

    public Status getStatus() {
        return status;
    }

    public RestStatus getRestStatus() {
        if (restStatus != null) {
            return restStatus;
        }

        if (status == Status.PASS) {
            return RestStatus.OK;
        } else {
            return RestStatus.UNAUTHORIZED;
        }
    }

    public String getRestStatusMessage() {
        return restStatusMessage;
    }

    public String getRedirectUri() {
        return redirectUri;
    }

    public static class DebugInfo implements ToXContentObject, Document<DebugInfo> {
        private final String authcMethod;
        private final boolean success;
        private final String message;
        private final Map<String, Object> details;

        public DebugInfo(String authcMethod, boolean success, String message) {
            this.authcMethod = authcMethod;
            this.success = success;
            this.message = message;
            this.details = null;
        }

        public DebugInfo(String authcMethod, boolean success, String message, Map<String, Object> details) {
            this.authcMethod = authcMethod;
            this.success = success;
            this.message = message;
            this.details = details != null ? ImmutableMap.of(details) : null;
        }

        public String getAuthcMethod() {
            return authcMethod;
        }

        public boolean isSuccess() {
            return success;
        }

        public String getMessage() {
            return message;
        }

        public Map<String, Object> getDetails() {
            return details;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("method", authcMethod);
            builder.field("success", success);
            builder.field("message", message);

            if (details != null && details.size() > 0) {
                // Use DocWriter and rawField to convert even unknown value types to String; by just using field,
                // the XContentBuilder would throw an exception for unknown value types
                builder.rawField("details", new ByteArrayInputStream(DocWriter.json().writeAsBytes(details)), XContentType.JSON);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public Object toBasicObject() {
            return ImmutableMap.ofNonNull("method", authcMethod, "success", success, "message", message, "details",
                    details != null && details.size() > 0 ? details : null);
        }

    }

    public List<DebugInfo> getDebug() {
        return debug;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (restStatus != null) {
            builder.field("status", restStatus);
        }

        if (restStatusMessage != null) {
            builder.field("error", restStatusMessage);
        }

        if (debug != null) {
            builder.field("debug", debug);
        }

        if (redirectUri != null) {
            builder.field("redirect_uri", redirectUri);
        }

        if (headers != null) {
            builder.field("headers", headers);
        }

        builder.endObject();
        return builder;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    @Override
    public Object toBasicObject() {
        return ImmutableMap.ofNonNull("status", restStatus, "error", restStatusMessage, "debug", debug, "redirect_uri", redirectUri, "headers",
                headers);
    }

}
