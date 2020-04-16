package com.floragunn.searchguard.auth;

import java.util.Collections;
import java.util.List;

public class AuthorizationDomain {

    private final AuthorizationBackend authorizationBackend;

    private final List<String> skippedUsers;

    public AuthorizationDomain(AuthorizationBackend authorizationBackend,
                               List<String> skippedUsers) {
        this.authorizationBackend = authorizationBackend;
        this.skippedUsers = skippedUsers;
    }

    public AuthorizationBackend getAuthorizationBackend() {
        return authorizationBackend;
    }

    public List<String> getSkippedUsers() {
        return Collections.unmodifiableList(skippedUsers);
    }
}
