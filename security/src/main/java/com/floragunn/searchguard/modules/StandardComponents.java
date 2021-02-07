/*
 * Copyright 2020-2021 floragunn GmbH
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

package com.floragunn.searchguard.modules;

import com.floragunn.searchguard.auth.AuthFailureListener;
import com.floragunn.searchguard.auth.HTTPAuthenticator;
import com.floragunn.searchguard.auth.api.AuthenticationBackend;
import com.floragunn.searchguard.auth.api.AuthorizationBackend;
import com.floragunn.searchguard.auth.internal.NoOpAuthenticationBackend;
import com.floragunn.searchguard.auth.internal.NoOpAuthorizationBackend;
import com.floragunn.searchguard.auth.limiting.AddressBasedRateLimiter;
import com.floragunn.searchguard.auth.limiting.UserNameBasedRateLimiter;
import com.floragunn.searchguard.auth.session.ApiAuthenticationFrontend;
import com.floragunn.searchguard.http.HTTPBasicAuthenticator;
import com.floragunn.searchguard.http.HTTPClientCertAuthenticator;
import com.floragunn.searchguard.http.HTTPProxyAuthenticator;
import com.floragunn.searchguard.http.HTTPProxyAuthenticator2;
import com.floragunn.searchguard.http.LinkApiAuthenticationFrontend;

public class StandardComponents {

    public static final SearchGuardComponentRegistry<AuthenticationBackend> authcBackends = new SearchGuardComponentRegistry<>(
            AuthenticationBackend.class)//
                    .add("noop", NoOpAuthenticationBackend.class)//
                    .add("ldap", "com.floragunn.dlic.auth.ldap.backend.LDAPAuthenticationBackend")//
                    .add("ldap2", "com.floragunn.dlic.auth.ldap2.LDAPAuthenticationBackend2")//
                    .seal();

    public static final SearchGuardComponentRegistry<AuthorizationBackend> authzBackends = new SearchGuardComponentRegistry<>(
            AuthorizationBackend.class)//
                    .add("noop", NoOpAuthorizationBackend.class)//
                    .add("ldap", "com.floragunn.dlic.auth.ldap.backend.LDAPAuthorizationBackend")//
                    .add("ldap2", "com.floragunn.dlic.auth.ldap2.LDAPAuthorizationBackend2")//
                    .seal();

    public static final SearchGuardComponentRegistry<HTTPAuthenticator> httpAuthenticators = new SearchGuardComponentRegistry<>(
            HTTPAuthenticator.class)//
                    .add("basic", HTTPBasicAuthenticator.class)//
                    .add("proxy", HTTPProxyAuthenticator.class)//
                    .add("proxy2", HTTPProxyAuthenticator2.class)//
                    .add("clientcert", HTTPClientCertAuthenticator.class)//
                    .add("kerberos", "com.floragunn.dlic.auth.http.kerberos.HTTPSpnegoAuthenticator")//
                    .add("jwt", "com.floragunn.dlic.auth.http.jwt.HTTPJwtAuthenticator")//
                    .add("openid", "com.floragunn.dlic.auth.http.jwt.keybyoidc.HTTPJwtKeyByOpenIdConnectAuthenticator")//
                    .add("saml", "com.floragunn.dlic.auth.http.saml.HTTPSamlAuthenticator")//
                    .seal();
    
    public static final SearchGuardComponentRegistry<ApiAuthenticationFrontend> apiAuthenticationFrontends = new SearchGuardComponentRegistry<>(
            ApiAuthenticationFrontend.class)//
                    .add("basic", HTTPBasicAuthenticator.class)//
                    .add("link", LinkApiAuthenticationFrontend.class)//
                    .add("saml", "com.floragunn.searchguard.enterprise.auth.saml.SamlAuthenticator")//
                    .add("oidc", "com.floragunn.searchguard.enterprise.auth.oidc.OidcAuthenticator")//
                    .seal();   

    public static final SearchGuardComponentRegistry<AuthFailureListener> authFailureListeners = new SearchGuardComponentRegistry<>(
            AuthFailureListener.class)//
                    .add("ip", AddressBasedRateLimiter.class)//
                    .add("username", UserNameBasedRateLimiter.class)//
                    .seal();
}
