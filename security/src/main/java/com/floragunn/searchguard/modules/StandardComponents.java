package com.floragunn.searchguard.modules;

import com.floragunn.searchguard.auth.AuthenticationBackend;
import com.floragunn.searchguard.auth.AuthorizationBackend;
import com.floragunn.searchguard.auth.internal.NoOpAuthenticationBackend;
import com.floragunn.searchguard.auth.internal.NoOpAuthorizationBackend;

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
}
