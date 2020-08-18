package com.floragunn.searchguard.authtoken;

import java.nio.file.Path;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.Settings;

import com.floragunn.searchguard.auth.AuthenticationBackend;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;

/**
 * TODO audience claim https://stackoverflow.com/questions/28418360/jwt-json-web-token-audience-aud-versus-client-id-whats-the-difference 
 *
 */
public class AuthTokenAuthenticationBackend implements AuthenticationBackend {

    private Settings settings;
    private AuthTokenService authTokenService;

    public AuthTokenAuthenticationBackend(Settings settings, Path configPath, AuthTokenService authTokenService) {
        this.settings = settings;
    }

    @Override
    public String getType() {
        return "auth_token";
    }

    @Override
    public User authenticate(AuthCredentials credentials) throws ElasticsearchSecurityException {
        try {
            AuthToken authToken = authTokenService.getByClaims(credentials.getClaims());
            
            
        } catch (NoSuchAuthTokenException | InvalidTokenException e) {
            throw new ElasticsearchSecurityException(e.getMessage(), e);
        }
    }

    @Override
    public boolean exists(User user) {
        // TODO Auto-generated method stub
        return false;
    }

}
