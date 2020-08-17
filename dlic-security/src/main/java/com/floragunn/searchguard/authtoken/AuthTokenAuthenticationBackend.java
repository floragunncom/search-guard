package com.floragunn.searchguard.authtoken;

import org.elasticsearch.ElasticsearchSecurityException;

import com.floragunn.searchguard.auth.AuthenticationBackend;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;

public class AuthTokenAuthenticationBackend implements AuthenticationBackend     {

    @Override
    public String getType() {
        return "auth_token";
    }

    @Override
    public User authenticate(AuthCredentials credentials) throws ElasticsearchSecurityException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean exists(User user) {
        // TODO Auto-generated method stub
        return false;
    }

}
