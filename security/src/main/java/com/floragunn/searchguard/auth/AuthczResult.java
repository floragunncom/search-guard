/*
 * Copyright 2015-2020 floragunn GmbH
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
package com.floragunn.searchguard.auth;

import org.elasticsearch.rest.RestStatus;

import com.floragunn.searchguard.user.User;

public class AuthczResult {
    public static final AuthczResult STOP = new AuthczResult(Status.STOP);
    public static final AuthczResult PASS_ANONYMOUS = new AuthczResult(User.ANONYMOUS, Status.PASS);
    public static final AuthczResult PASS_WITHOUT_AUTH = new AuthczResult(null, Status.PASS);

    public static AuthczResult stop(RestStatus restStatus, String message) {
        return new AuthczResult(Status.STOP, restStatus, message);
    }
    
    public static AuthczResult pass(User user) {
        return new AuthczResult(user, Status.PASS);
    }

    private final User user;
    private final Status status;
    private final RestStatus restStatus;
    private final String restStatusMessage;

    public AuthczResult(User user, Status status) {
        this.user = user;
        this.status = status;
        this.restStatus = null;
        this.restStatusMessage = null;
    }

    public AuthczResult(Status status) {
        this.user = null;
        this.status = status;
        this.restStatus = null;
        this.restStatusMessage = null;
    }

    public AuthczResult(Status status, RestStatus restStatus, String restStatusMessage) {
        this.user = null;
        this.status = status;
        this.restStatus = restStatus;
        this.restStatusMessage = restStatusMessage;
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
        return restStatus;
    }

    public String getRestStatusMessage() {
        return restStatusMessage;
    }
}
