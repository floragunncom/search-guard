/*
 * Copyright 2022 floragunn GmbH
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
package com.floragunn.searchguard.authc;

import com.floragunn.searchguard.authc.base.AuthcResult;

public class CredentialsException extends Exception {

    private static final long serialVersionUID = 4533702414070859512L;

    private AuthcResult.DebugInfo debugInfo;

    public CredentialsException(String message, Throwable cause) {
        super(message, cause);
    }

    public CredentialsException(String message) {
        super(message);
    }

    public CredentialsException(String message, AuthcResult.DebugInfo debugInfo, Throwable cause) {
        super(message, cause);
        this.debugInfo = debugInfo;
    }

    public CredentialsException(String message, AuthcResult.DebugInfo debugInfo) {
        super(message);
        this.debugInfo = debugInfo;
    }

    public CredentialsException(AuthcResult.DebugInfo debugInfo, Throwable cause) {
        super(debugInfo.getMessage(), cause);
        this.debugInfo = debugInfo;
    }

    public CredentialsException(AuthcResult.DebugInfo debugInfo) {
        super(debugInfo.getMessage());
        this.debugInfo = debugInfo;
    }

    public AuthcResult.DebugInfo getDebugInfo() {
        return debugInfo;
    }

}
