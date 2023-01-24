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

public class NoSuchSessionException extends Exception {

    private static final long serialVersionUID = -343178809366694796L;

    public NoSuchSessionException(String id) {
        super("Unknown session token: " + id);
    }

    public NoSuchSessionException(String id, Throwable cause) {
        super("Unknown session token: " + id, cause);
    }

}
