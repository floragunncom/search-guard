package com.floragunn.searchguard.sgconf;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.searchguard.auth.AuthFailureListener;
import com.floragunn.searchguard.auth.AuthenticationDomain;
import com.floragunn.searchguard.auth.AuthorizationDomain;
import com.floragunn.searchguard.auth.blocking.ClientBlockRegistry;
import com.google.common.collect.Multimap;

public abstract class DynamicConfigModel {
    
    protected final Logger log = LogManager.getLogger(this.getClass());
    public abstract SortedSet<AuthenticationDomain> getRestAuthenticationDomains();
    public abstract Set<AuthorizationDomain> getRestAuthorizationDomains();
    public abstract SortedSet<AuthenticationDomain> getTransportAuthenticationDomains();
    public abstract Set<AuthorizationDomain> getTransportAuthorizationDomains();
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
    public abstract String getFieldAnonymizationSalt2();    
    public abstract boolean isKibanaRbacEnabled();
    
    public abstract List<AuthFailureListener> getIpAuthFailureListeners();
    public abstract Multimap<String, AuthFailureListener> getAuthBackendFailureListeners();
    public abstract List<ClientBlockRegistry<InetAddress>> getIpClientBlockRegistries();
    public abstract Multimap<String, ClientBlockRegistry<String>> getAuthBackendClientBlockRegistries();
    public abstract Map<String, Object> getAuthTokenProviderConfig();
    

}
