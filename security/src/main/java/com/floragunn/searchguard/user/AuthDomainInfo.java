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

package com.floragunn.searchguard.user;

public class AuthDomainInfo {

    public static AuthDomainInfo UNKNOWN = new AuthDomainInfo(null, null, null);
    public static AuthDomainInfo ANON = AuthDomainInfo.forAuthenticatorType("none");
    public static AuthDomainInfo TLS_CERT = AuthDomainInfo.forAuthenticatorType("tls_cert");
    public static AuthDomainInfo IMPERSONATION_TLS = AuthDomainInfo.forAuthenticatorType("impersonation+tls_cert");
    public static AuthDomainInfo STORED_AUTH = AuthDomainInfo.forAuthenticatorType("stored_auth");
    public static AuthDomainInfo INJECTED = AuthDomainInfo.forAuthenticatorType("ext");

    private final String authDomainId;
    private final String authenticatorType;
    private final String authBackendType;

    private AuthDomainInfo(String authDomainId, String authenticatorType, String authBackendType) {
        this.authDomainId = authDomainId;
        this.authBackendType = authBackendType;
        this.authenticatorType = authenticatorType;
    }

    public String getAuthDomainId() {
        return authDomainId;
    }

    public AuthDomainInfo authDomainId(String authDomainId) {
        return new AuthDomainInfo(authDomainId, this.authenticatorType, this.authBackendType);
    }

    public String getAuthenticatorType() {
        return authenticatorType;
    }

    public AuthDomainInfo authenticatorType(String authenticatorType) {
        return new AuthDomainInfo(this.authDomainId, authenticatorType, this.authBackendType);
    }

    public String getAuthBackendType() {
        return authBackendType;
    }

    public AuthDomainInfo authBackendType(String authBackendType) {
        return new AuthDomainInfo(this.authDomainId, this.authenticatorType, authBackendType);
    }

    public AuthDomainInfo addAuthBackend(String authBackendType) {
        String newAuthBackendType = this.authBackendType;

        if (newAuthBackendType == null) {
            newAuthBackendType = authBackendType;
        } else {
            newAuthBackendType = authBackendType + "+" + newAuthBackendType;
        }

        return new AuthDomainInfo(this.authDomainId, this.authenticatorType, newAuthBackendType);
    }
    
    public AuthDomainInfo add(AuthDomainInfo other) {
        if (other == null || (other.authDomainId == null && other.authBackendType == null && other.authenticatorType == null)) {
            return this;
        } else {
            return new AuthDomainInfo(other.authDomainId != null ? other.authDomainId : this.authDomainId,
                    other.authenticatorType != null ? other.authenticatorType : this.authenticatorType,
                    other.authBackendType != null ? other.authBackendType : this.authBackendType);
        }
    }

    public boolean isUnknown() {
        return authenticatorType == null && authBackendType == null && authDomainId != null;
    }
    
    public static AuthDomainInfo forAuthDomainId(String authDomainId) {
        return new AuthDomainInfo(authDomainId, null, null);
    }

    public static AuthDomainInfo forAuthenticatorType(String authenticatorType) {
        return new AuthDomainInfo(null, authenticatorType, null);
    }

    @Override
    public String toString() {
        return "AuthDomainInfo [authDomainId=" + authDomainId + ", authenticatorType=" + authenticatorType + ", authBackendType=" + authBackendType
                + "]";
    }

    public String toInfoString() {
        if (this.authenticatorType != null && this.authBackendType != null) {
            return this.authenticatorType + "/" + this.authBackendType;
        } else if (this.authenticatorType != null) {
            return this.authenticatorType;
        } else if (this.authBackendType != null) {
            return "/" + this.authBackendType;
        } else {
            return "n/a";
        }
    }

    public static AuthDomainInfo from(User user) {
        String string = user.getAuthDomain();

        if (string == null || string.equals("n/a")) {
            return UNKNOWN;
        } else {
            int slash = string.indexOf('/');

            if (slash == -1) {
                return AuthDomainInfo.forAuthenticatorType(string);
            } else if (slash == 0) {
                return new AuthDomainInfo(null, null, string.substring(1));
            } else {
                return new AuthDomainInfo(null, string.substring(0, slash), string.substring(slash + 1));
            }
        }
    }

}
