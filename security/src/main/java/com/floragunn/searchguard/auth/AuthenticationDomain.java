/*
 * Copyright 2015-2017 floragunn GmbH
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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.floragunn.searchguard.auth.api.AuthenticationBackend;
import com.floragunn.searchguard.support.IPAddressCollection;

public class AuthenticationDomain<AuthenticatorType extends AuthenticationFrontend> implements Comparable<AuthenticationDomain<AuthenticatorType>> {

    private final String id;
    private final AuthenticationBackend backend;
    private final AuthenticatorType authenticator;
    private final int order;
    private final boolean challenge;
    private final List<String> skippedUsers;
    private final IPAddressCollection enabledOnlyForIps;

    public AuthenticationDomain(String id, final AuthenticationBackend backend, final AuthenticatorType authenticator, boolean challenge,
                                final int order, List<String> skippedUsers, IPAddressCollection enabledOnlyForIps) {
        super();
        this.id = id;
        this.backend = Objects.requireNonNull(backend);
        this.authenticator = authenticator;
        this.order = order;
        this.challenge = challenge;
        this.skippedUsers = skippedUsers;
        this.enabledOnlyForIps = enabledOnlyForIps;
    }

    public boolean isChallenge() {
        return challenge;
    }

    public AuthenticationBackend getBackend() {
        return backend;
    }

    public AuthenticatorType getHttpAuthenticator() {
        return authenticator;
    }

    public int getOrder() {
        return order;
    }

    @Override
    public String toString() {
        return "AuthenticationDomain [backend=" + backend + ", httpAuthenticator=" + authenticator + ", order=" + order + ", challenge="
                + challenge + "]";
    }

    @Override
    public int compareTo(final AuthenticationDomain<AuthenticatorType> o) {
        return Integer.compare(this.order, o.order);
    }

    public List<String> getSkippedUsers() {
        return Collections.unmodifiableList(skippedUsers);
    }

    public IPAddressCollection getEnabledOnlyForIps() {
        return enabledOnlyForIps;
    }

    public String getId() {
        return id;
    }

}