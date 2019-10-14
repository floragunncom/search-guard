package com.floragunn.searchguard.sgconf;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.searchguard.auth.AuthDomain;
import com.floragunn.searchguard.auth.AuthFailureListener;
import com.floragunn.searchguard.auth.AuthorizationBackend;
import com.floragunn.searchguard.auth.blocking.ClientBlockRegistry;
import com.floragunn.searchguard.auth.internal.InternalAuthenticationBackend;
import com.floragunn.searchguard.auth.internal.NoOpAuthenticationBackend;
import com.floragunn.searchguard.auth.internal.NoOpAuthorizationBackend;
import com.floragunn.searchguard.auth.limiting.AddressBasedRateLimiter;
import com.floragunn.searchguard.auth.limiting.UserNameBasedRateLimiter;
import com.floragunn.searchguard.http.HTTPBasicAuthenticator;
import com.floragunn.searchguard.http.HTTPClientCertAuthenticator;
import com.floragunn.searchguard.http.HTTPProxyAuthenticator;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

public abstract class DynamicConfigModel {
    
    protected final Logger log = LogManager.getLogger(this.getClass());
    public abstract SortedSet<AuthDomain> getRestAuthDomains();
    public abstract Set<AuthorizationBackend> getRestAuthorizers();
    public abstract SortedSet<AuthDomain> getTransportAuthDomains();
    public abstract Set<AuthorizationBackend> getTransportAuthorizers();
    public abstract String getTransportUsernameAttribute();
    public abstract boolean isAnonymousAuthenticationEnabled();
    public abstract boolean isXffEnabled();
    public abstract String getInternalProxies();
    public abstract String getRemoteIpHeader();
    public abstract boolean isRestAuthDisabled();
    public abstract boolean isInterTransportAuthDisabled();
    public abstract boolean isRespectRequestIndicesEnabled();
    public abstract String getKibanaServerUsername();
    public abstract String getKibanaIndexname();
    public abstract boolean isKibanaMultitenancyEnabled();
    public abstract boolean isDnfofEnabled();
    public abstract boolean isMultiRolespanEnabled();
    public abstract String getFilteredAliasMode();
    public abstract String getHostsResolverMode();
    public abstract boolean isDnfofForEmptyResultsEnabled();
    
    public abstract List<AuthFailureListener> getIpAuthFailureListeners();
    public abstract Multimap<String, AuthFailureListener> getAuthBackendFailureListeners();
    public abstract List<ClientBlockRegistry<InetAddress>> getIpClientBlockRegistries();
    public abstract Multimap<String, ClientBlockRegistry<String>> getAuthBackendClientBlockRegistries();
    
    protected final Map<String, String> authImplMap = new HashMap<>();

    public DynamicConfigModel() {
        super();
        
        authImplMap.put("intern_c", InternalAuthenticationBackend.class.getName());
        authImplMap.put("intern_z", NoOpAuthorizationBackend.class.getName());

        authImplMap.put("internal_c", InternalAuthenticationBackend.class.getName());
        authImplMap.put("internal_z", NoOpAuthorizationBackend.class.getName());

        authImplMap.put("noop_c", NoOpAuthenticationBackend.class.getName());
        authImplMap.put("noop_z", NoOpAuthorizationBackend.class.getName());

        authImplMap.put("ldap_c", "com.floragunn.dlic.auth.ldap.backend.LDAPAuthenticationBackend");
        authImplMap.put("ldap_z", "com.floragunn.dlic.auth.ldap.backend.LDAPAuthorizationBackend");
        
        authImplMap.put("ldap2_c", "com.floragunn.dlic.auth.ldap2.LDAPAuthenticationBackend2");
        authImplMap.put("ldap2_z", "com.floragunn.dlic.auth.ldap2.LDAPAuthorizationBackend2");

        authImplMap.put("basic_h", HTTPBasicAuthenticator.class.getName());
        authImplMap.put("proxy_h", HTTPProxyAuthenticator.class.getName());
        authImplMap.put("clientcert_h", HTTPClientCertAuthenticator.class.getName());
        authImplMap.put("kerberos_h", "com.floragunn.dlic.auth.http.kerberos.HTTPSpnegoAuthenticator");
        authImplMap.put("jwt_h", "com.floragunn.dlic.auth.http.jwt.HTTPJwtAuthenticator");
        authImplMap.put("openid_h", "com.floragunn.dlic.auth.http.jwt.keybyoidc.HTTPJwtKeyByOpenIdConnectAuthenticator");
        authImplMap.put("saml_h", "com.floragunn.dlic.auth.http.saml.HTTPSamlAuthenticator");
        
        authImplMap.put("ip_authFailureListener", AddressBasedRateLimiter.class.getName());
        authImplMap.put("username_authFailureListener", UserNameBasedRateLimiter.class.getName());
    }
    
    
    
}
