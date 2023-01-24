/*
 * Copyright 2020 floragunn GmbH
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

import org.elasticsearch.rest.RestStatus;

public class SessionCreationException extends Exception {

    private static final long serialVersionUID = -47600121877964762L;

    private RestStatus restStatus;

    public SessionCreationException(String message, RestStatus restStatus, Throwable cause) {
        super(message, cause);
        this.restStatus = restStatus;
    }

    public SessionCreationException(String message, RestStatus restStatus) {
        super(message);
        this.restStatus = restStatus;
    }

    public RestStatus getRestStatus() {
        return restStatus;
    }

    public void setRestStatus(RestStatus restStatus) {
        this.restStatus = restStatus;
    }

}
