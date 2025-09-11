/*
 * Copyright 2020-2022 by floragunn GmbH - All rights reserved
 * 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 
 * This software is free of charge for non-commercial and academic use. 
 * For commercial use in a production environment you have to obtain a license 
 * from https://floragunn.com
 * 
 */

package com.floragunn.searchguard.authtoken;

import java.util.concurrent.CompletableFuture;

import org.elasticsearch.ElasticsearchSecurityException;

import com.floragunn.searchguard.authc.AuthenticationDebugLogger;
import com.floragunn.searchguard.authc.AuthenticationDomain;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.authc.CredentialsException;
import com.floragunn.searchguard.authc.RequestMetaData;
import com.floragunn.searchguard.authc.rest.HttpAuthenticationFrontend;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.cstate.ComponentState;

public class AuthTokenAuthenticationDomain implements AuthenticationDomain<HttpAuthenticationFrontend> {

    private final AuthTokenService authTokenService;
    private final AuthTokenHttpJwtAuthenticator httpAuthenticator;
    private final ComponentState componentState;

    public AuthTokenAuthenticationDomain(AuthTokenService authTokenService) {
        this.authTokenService = authTokenService;
        this.httpAuthenticator = new AuthTokenHttpJwtAuthenticator(authTokenService);
        
        this.componentState = new ComponentState(0, "auth_domain", "sg_auth_token", AuthTokenAuthenticationDomain.class);
        this.componentState.addPart(this.httpAuthenticator.getComponentState());
        this.componentState.updateStateFromParts();
    }

    @Override
    public String getType() {
        return "sg_auth_token";
    }

    @Override
    public CompletableFuture<User> authenticate(AuthCredentials credentials, AuthenticationDebugLogger debug) {

        try {

            CompletableFuture<User> result = new CompletableFuture<User>();

            authTokenService.getByClaims(credentials.getClaims(), (authToken) -> {

                if (authToken.isRevoked()) {
                    result.completeExceptionally(new ElasticsearchSecurityException("Auth token " + authToken.getId() + " has been revoked"));
                } else if (authToken.getBase().getConfigVersions() == null && authToken.getRequestedPrivileges().isTotalWildcard()) {
                    // This auth token has no restrictions and no snapshotted base. We can use the current roles. Thus, we can completely initialize the user

                    result.complete(User.forUser(authToken.getUserName()).type(AuthTokenService.USER_TYPE_FULL_CURRENT_PERMISSIONS)
                            .authDomainInfo(credentials.getAuthDomainInfo().authBackendType(getType()))
                            .backendRoles(authToken.getBase().getBackendRoles()).searchGuardRoles(authToken.getBase().getSearchGuardRoles())
                            .specialAuthzConfig(authToken.getId()).attributes(authToken.getBase().getAttributes()).authzComplete().build());
                } else {
                    // This auth token has restrictions or must use the snapshotted config specified in authToken.getBase().getConfigVersions()
                    // Thus, we won't initialize a "normal" User object. Rather, the user object won't contain any roles, 
                    // as these would not refer to the current configuration. Code which is supposed to support auth tokens with frozen configuration,
                    // needs to use the SpecialPrivilegesEvaluationContextProvider API to retrieve the correct configuration

                    result.complete(User.forUser(authToken.getUserName()).authDomainInfo(credentials.getAuthDomainInfo().authBackendType(getType()))
                            .subName("AuthToken " + authToken.getTokenName() + " [" + authToken.getId() + "]").type(AuthTokenService.USER_TYPE)
                            .specialAuthzConfig(authToken.getId()).attributes(authToken.getBase().getAttributes()).authzComplete().build());
                }
            }, (noSuchAuthTokenException) -> {
                result.completeExceptionally(new ElasticsearchSecurityException(noSuchAuthTokenException.getMessage(), noSuchAuthTokenException));
            }, (e) -> {
                result.completeExceptionally(e);
            });

            return result;

        } catch (InvalidTokenException e) {
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public AuthTokenHttpJwtAuthenticator getFrontend() {
        return httpAuthenticator;
    }

    @Override
    public String getId() {
        return getType();
    }

    @Override
    public boolean accept(RequestMetaData<?> request) {
        return true;
    }

    @Override
    public boolean accept(AuthCredentials authCredentials) {
        return true;
    }

    @Override
    public boolean isEnabled() {
        return authTokenService.isEnabled();
    }

    @Override
    public CompletableFuture<User> impersonate(User originalUser, AuthCredentials authCredentials)
            throws AuthenticatorUnavailableException, CredentialsException {
        return null;
    }

    @Override
    public boolean cacheUser() {
        return false;
    }
    
    @Override
    public String toString() {
        return getType();
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

}
