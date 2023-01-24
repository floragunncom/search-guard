/*
 * Copyright 2023 floragunn GmbH
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
package com.floragunn.searchguard.ssl.util.config;

public class GenericSSLConfigException extends Exception {

    private static final long serialVersionUID = 3774103067927533078L;

    public GenericSSLConfigException() {
        super();
    }

    public GenericSSLConfigException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public GenericSSLConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public GenericSSLConfigException(String message) {
        super(message);
    }

    public GenericSSLConfigException(Throwable cause) {
        super(cause);
    }

}
