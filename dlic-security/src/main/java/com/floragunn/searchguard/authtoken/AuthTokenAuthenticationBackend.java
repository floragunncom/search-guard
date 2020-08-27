package com.floragunn.searchguard.authtoken;

import org.elasticsearch.ElasticsearchSecurityException;

import com.floragunn.searchguard.auth.AuthenticationBackend;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;

/**
 * TODO audience claim https://stackoverflow.com/questions/28418360/jwt-json-web-token-audience-aud-versus-client-id-whats-the-difference 
 *
 */
public class AuthTokenAuthenticationBackend implements AuthenticationBackend {

    private AuthTokenService authTokenService;

    public AuthTokenAuthenticationBackend(AuthTokenService authTokenService) {
        this.authTokenService = authTokenService;
    }

    @Override
    public String getType() {
        return "sg_auth_token";
    }

    @Override
    public User authenticate(AuthCredentials credentials) throws ElasticsearchSecurityException {
        try {
            AuthToken authToken = authTokenService.getByClaims(credentials.getClaims());

            return User.forUser(authToken.getUserName()).subName(authToken.getTokenName() + "[" + authToken.getId() + "]")
                    .type(AuthTokenService.USER_TYPE).specialAuthzConfig(authToken.getId()).authzComplete().build();

        } catch (NoSuchAuthTokenException | InvalidTokenException e) {
            throw new ElasticsearchSecurityException(e.getMessage(), e);
        }
    }

    @Override
    public boolean exists(User user) {
        // This is only related to impersonation. Auth tokens don't support impersonation.
        return false;
    }

    @Override
    public UserCachingPolicy userCachingPolicy() {
        return UserCachingPolicy.NEVER;
    }
}
