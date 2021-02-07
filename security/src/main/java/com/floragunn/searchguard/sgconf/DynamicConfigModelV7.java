package com.floragunn.searchguard.sgconf;

import java.net.InetAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Supplier;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentType;

import com.floragunn.codova.validation.VariableResolvers;
import com.floragunn.searchguard.auth.AuthFailureListener;
import com.floragunn.searchguard.auth.AuthenticationDomain;
import com.floragunn.searchguard.auth.AuthenticationFrontend;
import com.floragunn.searchguard.auth.AuthorizationDomain;
import com.floragunn.searchguard.auth.Destroyable;
import com.floragunn.searchguard.auth.HTTPAuthenticator;
import com.floragunn.searchguard.auth.api.AuthenticationBackend;
import com.floragunn.searchguard.auth.api.AuthorizationBackend;
import com.floragunn.searchguard.auth.blocking.ClientBlockRegistry;
import com.floragunn.searchguard.auth.internal.NoOpAuthenticationBackend;
import com.floragunn.searchguard.auth.session.ApiAuthenticationFrontend;
import com.floragunn.searchguard.modules.SearchGuardModulesRegistry;
import com.floragunn.searchguard.modules.state.ComponentState;
import com.floragunn.searchguard.modules.state.ComponentStateProvider;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.sgconf.impl.v7.ConfigV7;
import com.floragunn.searchguard.sgconf.impl.v7.ConfigV7.Authc;
import com.floragunn.searchguard.sgconf.impl.v7.ConfigV7.AuthcDomain;
import com.floragunn.searchguard.sgconf.impl.v7.ConfigV7.Authz;
import com.floragunn.searchguard.sgconf.impl.v7.ConfigV7.AuthzDomain;
import com.floragunn.searchguard.sgconf.impl.v7.FrontendConfig;
import com.floragunn.searchguard.support.IPAddressCollection;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

public class DynamicConfigModelV7 extends DynamicConfigModel implements ComponentStateProvider {
    private final ConfigV7 config;
    private final Settings esSettings;
    private final Path configPath;
    private SortedSet<AuthenticationDomain<HTTPAuthenticator>> restAuthenticationDomains;
    private SortedSet<AuthenticationDomain<HTTPAuthenticator>> transportAuthenticationDomains;
    private Map<String, List<AuthenticationDomain<ApiAuthenticationFrontend>>> apiAuthenticationDomainMap;
    private Set<AuthorizationDomain> restAuthorizationDomains;
    private Set<AuthorizationDomain> transportAuthorizationDomains;
    private List<Destroyable> destroyableComponents;
    private final SearchGuardModulesRegistry modulesRegistry;
    private final VariableResolvers configVariableProviders;
    
    private List<AuthFailureListener> ipAuthFailureListeners;
    private Multimap<String, AuthFailureListener> authBackendFailureListeners;
    private List<ClientBlockRegistry<InetAddress>> ipClientBlockRegistries;
    private Multimap<String, ClientBlockRegistry<String>> authBackendClientBlockRegistries;
    private final ComponentState componentState = new ComponentState(2, null, "sg_config", DynamicConfigModelV7.class);

    private SgDynamicConfiguration<FrontendConfig> frontendConfig;
    private List<Supplier<List<AuthenticationDomain<HTTPAuthenticator>>>> authenticationDomainInjectors;
    
    public DynamicConfigModelV7(ConfigV7 config, SgDynamicConfiguration<FrontendConfig> frontendConfig, Settings esSettings, Path configPath,
            SearchGuardModulesRegistry modulesRegistry, List<Supplier<List<AuthenticationDomain<HTTPAuthenticator>>>> authenticationDomainInjectors,
            VariableResolvers configVariableProviders) {
        super();
        this.config = config;
        this.esSettings = esSettings;
        this.configPath = configPath;
        this.modulesRegistry = modulesRegistry;
        this.frontendConfig = frontendConfig;
        this.authenticationDomainInjectors = authenticationDomainInjectors;
        this.configVariableProviders = configVariableProviders;
        buildAAA();
        
        if (config != null && config.dynamic != null && !config.dynamic.multi_rolespan_enabled) {
            log.error("The option multi_rolespan_enabled is no longer supported; from now on the privilege evaluation will always work like multi_rolespan_enabled was set to true");
        }
    }

    @Override
    public SortedSet<AuthenticationDomain<HTTPAuthenticator>> getRestAuthenticationDomains() {
        return Collections.unmodifiableSortedSet(restAuthenticationDomains);
    }

    @Override
    public Set<AuthorizationDomain> getRestAuthorizationDomains() {
        return Collections.unmodifiableSet(restAuthorizationDomains);
    }

    @Override
    public SortedSet<AuthenticationDomain<HTTPAuthenticator>> getTransportAuthenticationDomains() {
        return Collections.unmodifiableSortedSet(transportAuthenticationDomains);
    }

    @Override
    public Set<AuthorizationDomain> getTransportAuthorizationDomains() {
        return Collections.unmodifiableSet(transportAuthorizationDomains);
    }

    @Override
    public Map<String, List<AuthenticationDomain<ApiAuthenticationFrontend>>> getApiAuthenticationDomainMap() {
        return apiAuthenticationDomainMap;
    }

    @Override
    public String getTransportUsernameAttribute() {
        return config.dynamic.transport_userrname_attribute;
    }
    @Override
    public boolean isAnonymousAuthenticationEnabled() {
        return config.dynamic.http.anonymous_auth_enabled;
    }
    @Override
    public boolean isXffEnabled() {
        return config.dynamic.http.xff.enabled;
    }
    @Override
    public String getInternalProxies() {
        return config.dynamic.http.xff.internalProxies;
    }

    @Override
    public String getRemoteIpHeader() {
        return config.dynamic.http.xff.remoteIpHeader;
    }

    @Override
    public String getFieldAnonymizationSalt2() {
        return config.dynamic.field_anonymization_salt2;
    }
    
    @Override
    public boolean isRespectRequestIndicesEnabled() {
        return config.dynamic.respect_request_indices_options;
    }
    @Override
    public String getKibanaServerUsername() {
        return config.dynamic.kibana.server_username;
    }
    @Override
    public String getKibanaIndexname() {
        return config.dynamic.kibana.index;
    }
    @Override
    public boolean isKibanaMultitenancyEnabled() {
        return config.dynamic.kibana.multitenancy_enabled;
    }
    @Override
    public boolean isDnfofEnabled() {
        return config.dynamic.do_not_fail_on_forbidden;
    }
    @Override
    public String getHostsResolverMode() {
        return config.dynamic.hosts_resolver_mode;
    }
    
    @Override
    public boolean isDnfofForEmptyResultsEnabled() {
        return config.dynamic.do_not_fail_on_forbidden_empty;
    }
    
    @Override
    public boolean isKibanaRbacEnabled() {
        return config.dynamic.kibana.rbac_enabled;
    }
    
    public boolean isAuthDebugEnabled() {
        return config.dynamic.debug != null && config.dynamic.debug.booleanValue();
    }
    
    @Override
    public List<AuthFailureListener> getIpAuthFailureListeners() {
        return Collections.unmodifiableList(ipAuthFailureListeners);
    }
    
    @Override
    public Multimap<String, AuthFailureListener> getAuthBackendFailureListeners() {
        return Multimaps.unmodifiableMultimap(authBackendFailureListeners);
    }
    
    @Override
    public List<ClientBlockRegistry<InetAddress>> getIpClientBlockRegistries() {
        return Collections.unmodifiableList(ipClientBlockRegistries);
    }
    
    @Override
    public Multimap<String, ClientBlockRegistry<String>> getAuthBackendClientBlockRegistries() {
        return Multimaps.unmodifiableMultimap(authBackendClientBlockRegistries);
    }

    private void buildAAA() {
        
        final SortedSet<AuthenticationDomain<HTTPAuthenticator>> restAuthenticationDomains0 = new TreeSet<>();
        final SortedSet<AuthenticationDomain<HTTPAuthenticator>> transportAuthenticationDomains0 = new TreeSet<>();
        final List<AuthenticationDomain<ApiAuthenticationFrontend>> apiAuthenticationDomains = new ArrayList<>();

        final Set<AuthorizationDomain> restAuthorizationDomains0 = new HashSet<>();
        final Set<AuthorizationDomain> transportAuthorizationDomains0 = new HashSet<>();
        final List<Destroyable> destroyableComponents0 = new LinkedList<>();
        final List<AuthFailureListener> ipAuthFailureListeners0 = new ArrayList<>();
        final Multimap<String, AuthFailureListener> authBackendFailureListeners0 = ArrayListMultimap.create();
        final List<ClientBlockRegistry<InetAddress>> ipClientBlockRegistries0 = new ArrayList<>();
        final Multimap<String, ClientBlockRegistry<String>> authBackendClientBlockRegistries0 = ArrayListMultimap.create();
                
        if (config == null) {
            // We are not yet initialized
            return;
        }

        boolean hasBasicApiAuthenticationDomains = false;
        
        final Authz authzDyn = config.dynamic.authz;

        for (final Entry<String, AuthzDomain> ad : authzDyn.getDomains().entrySet()) {
            final boolean httpEnabled = ad.getValue().http_enabled;
            final boolean transportEnabled = ad.getValue().transport_enabled;


            if (httpEnabled || transportEnabled) {
                ComponentState domainState = componentState.getOrCreatePart("authz_domain", ad.getKey());
                
                try {

                    String authzBackendClazz = ad.getValue().authorization_backend.type;
                    Settings authorizationBackendSettings = Settings.builder().put(esSettings)
                            .put(Settings.builder().loadFromSource(ad.getValue().authorization_backend.configAsJson(), XContentType.JSON).build())
                            .build();

                    AuthorizationBackend authorizationBackend = modulesRegistry.getAuthorizationBackends().getInstance(authzBackendClazz,
                            authorizationBackendSettings, configPath);
         
                    List<String> skippedUsers = ad.getValue().skipped_users;
                    if (httpEnabled) {
                        AuthorizationDomain domain = new AuthorizationDomain(authorizationBackend, skippedUsers);
                        restAuthorizationDomains0.add(domain);
                    }

                    if (transportEnabled) {
                        AuthorizationDomain domain = new AuthorizationDomain(authorizationBackend, skippedUsers);
                        transportAuthorizationDomains0.add(domain);
                    }
                    
                    if (authorizationBackend instanceof Destroyable) {
                        // XXX this is dangerous for components which were not constructed here 
                        destroyableComponents0.add((Destroyable) authorizationBackend);
                    }
                    
                    domainState.setInitialized();
                } catch (final Exception e) {
                    log.error("Unable to initialize AuthorizationBackend {} due to {}", ad, e.toString(),e);
                    domainState.setFailed(e);
                }
            }
        }

        final Authc authcDyn = config.dynamic.authc;

        for (final Entry<String, AuthcDomain> ad : authcDyn.getDomains().entrySet()) {
            final boolean httpEnabled = ad.getValue().http_enabled;
            final boolean transportEnabled = ad.getValue().transport_enabled;
            final Boolean sessionEnabled = ad.getValue().session_enabled;
            
            if (httpEnabled || transportEnabled || (sessionEnabled != null && sessionEnabled.booleanValue())) {
                ComponentState domainState = componentState.getOrCreatePart("authc_domain", ad.getKey());

                try {
                    String authBackendClazz = ad.getValue().authentication_backend.type;
                    Settings authenticationBackendSettings = Settings.builder()
                            .put(esSettings)
                            .put(Settings.builder().loadFromSource(ad.getValue().authentication_backend.configAsJson(), XContentType.JSON).build()).build();
                    
                    AuthenticationBackend authenticationBackend = modulesRegistry.getAuthenticationBackends().getInstance(authBackendClazz,
                            authenticationBackendSettings, configPath);

                    String httpAuthenticatorType = ad.getValue().http_authenticator.type; //no default
                    HTTPAuthenticator httpAuthenticator = null;
                    
                    if (httpAuthenticatorType != null) {
                        Map<String, Object> frontendConfig = ad.getValue().http_authenticator.config;
                      
                        AuthenticationFrontend.Context context = new AuthenticationFrontend.Context(configPath, esSettings, configVariableProviders);
                        
                        httpAuthenticator = modulesRegistry.getHttpAuthenticators().getInstance(httpAuthenticatorType, frontendConfig,
                                context);
                        
                    }

                    IPAddressCollection enabledOnlyForHosts = null;
                    
                    if (ad.getValue().enabled_only_for_ips != null && ad.getValue().enabled_only_for_ips.size() > 0) {
                        enabledOnlyForHosts = IPAddressCollection.create(ad.getValue().enabled_only_for_ips);
                    }
                    
                    final AuthenticationDomain<HTTPAuthenticator> _ad = new AuthenticationDomain<HTTPAuthenticator>(ad.getKey(),
                            authenticationBackend, httpAuthenticator, ad.getValue().http_authenticator.challenge, ad.getValue().order,
                            ad.getValue().skip_users, enabledOnlyForHosts);

                    if (httpEnabled && _ad.getHttpAuthenticator() != null) {
                        restAuthenticationDomains0.add(_ad);
                    }

                    if (transportEnabled) {
                        transportAuthenticationDomains0.add(_ad);
                    }

                    if (sessionEnabled == null || sessionEnabled.booleanValue()) {
                        if (httpAuthenticator instanceof ApiAuthenticationFrontend) {
                            AuthenticationDomain<ApiAuthenticationFrontend> authDomain = new AuthenticationDomain<>(ad.getKey(),
                                    authenticationBackend, (ApiAuthenticationFrontend) httpAuthenticator, ad.getValue().http_authenticator.challenge,
                                    ad.getValue().order, ad.getValue().skip_users, enabledOnlyForHosts);

                            if ("basic".equals(httpAuthenticator.getType())) {
                                hasBasicApiAuthenticationDomains = true;
                            }

                            apiAuthenticationDomains.add(authDomain);
                        } else if (sessionEnabled != null) {
                            // Only log an error of this is explicitly enabled
                            log.error("Cannot use authenticator " + httpAuthenticator.getType() + " for session auth. This type is not supported");
                            domainState.setMessage("Cannot use authenticator for session auth. This type is not supported");
                        }
                    }

                    if (httpAuthenticator instanceof Destroyable) {
                        // XXX this is dangerous for components which were not constructed here 
                        destroyableComponents0.add((Destroyable) httpAuthenticator);
                    }
                    
                    if (authenticationBackend instanceof Destroyable) {
                        // XXX this is dangerous for components which were not constructed here 
                        destroyableComponents0.add((Destroyable) authenticationBackend);
                    }
                    
                    domainState.setInitialized();
                    
                } catch (final Exception e) {
                    log.error("Unable to initialize auth domain {} due to {}", ad, e.toString(), e);
                    domainState.setFailed(e);
                }

            }
        }
        
        restAuthenticationDomains0.addAll(getInjectedAuthenticationDomains());
        
        if (!apiAuthenticationDomains.isEmpty() && frontendConfig.getCEntries().isEmpty()) {
            // Simplify configuration for users who only use username/password based authentication:
            // These can leave sg_frontend_config.yml empty, we add basic auth for them here.
            if (hasBasicApiAuthenticationDomains) {
                frontendConfig.getCEntries().put("default", FrontendConfig.BASIC);
            }
        }
        
        List<Destroyable> originalDestroyableComponents = destroyableComponents;
        
        restAuthenticationDomains = Collections.unmodifiableSortedSet(restAuthenticationDomains0);
        transportAuthenticationDomains = Collections.unmodifiableSortedSet(transportAuthenticationDomains0);
        this.apiAuthenticationDomainMap = buildApiAuthenticationDomainMap(apiAuthenticationDomains);
        
        restAuthorizationDomains = Collections.unmodifiableSet(restAuthorizationDomains0);
        transportAuthorizationDomains = Collections.unmodifiableSet(transportAuthorizationDomains0);
        
        destroyableComponents = Collections.unmodifiableList(destroyableComponents0);
        
        if(originalDestroyableComponents != null) {
            destroyDestroyables(originalDestroyableComponents);
        }

        createAuthFailureListeners(ipAuthFailureListeners0,
                authBackendFailureListeners0, ipClientBlockRegistries0, authBackendClientBlockRegistries0, destroyableComponents0);
        
        ipAuthFailureListeners = Collections.unmodifiableList(ipAuthFailureListeners0);
        ipClientBlockRegistries = Collections.unmodifiableList(ipClientBlockRegistries0);
        authBackendClientBlockRegistries = Multimaps.unmodifiableMultimap(authBackendClientBlockRegistries0);
        authBackendFailureListeners = Multimaps.unmodifiableMultimap(authBackendFailureListeners0);
    }
    
    private Collection<AuthenticationDomain<HTTPAuthenticator>> getInjectedAuthenticationDomains() {
        if (this.authenticationDomainInjectors.isEmpty()) {
            return Collections.emptyList();
        }
        
        List<AuthenticationDomain<HTTPAuthenticator>> result = new ArrayList<>();
        
        for (Supplier<List<AuthenticationDomain<HTTPAuthenticator>>> injector : this.authenticationDomainInjectors) {
            result.addAll(injector.get());
        }
        
        return result;
    }

    private Map<String, List<AuthenticationDomain<ApiAuthenticationFrontend>>> buildApiAuthenticationDomainMap(List<AuthenticationDomain<ApiAuthenticationFrontend>> globalApiAuthenticationDomains) {
  
        Map<String, List<AuthenticationDomain<ApiAuthenticationFrontend>>> result = new HashMap<>();

        if (frontendConfig != null) {
            for (Map.Entry<String, FrontendConfig> entry : frontendConfig.getCEntries().entrySet()) {
                FrontendConfig frontendConfig = entry.getValue();

                List<AuthenticationDomain<ApiAuthenticationFrontend>> apiAuthenticationDomains = new ArrayList<>(
                        globalApiAuthenticationDomains.size() + frontendConfig.getAuthcz().size());

                apiAuthenticationDomains.addAll(globalApiAuthenticationDomains);

                for (FrontendConfig.Authcz authcz : frontendConfig.getAuthcz()) {
                    if (authcz.getAuthenticationFrontend() instanceof ApiAuthenticationFrontend && authcz.isEnabled()) {
                        AuthenticationDomain<ApiAuthenticationFrontend> authDomain = new AuthenticationDomain<>(authcz.getId(),
                                NoOpAuthenticationBackend.INSTANCE, (ApiAuthenticationFrontend) authcz.getAuthenticationFrontend(), false,
                                Integer.MAX_VALUE, Collections.emptyList(), null);

                        apiAuthenticationDomains.add(authDomain);
                    }
                }

                Collections.sort(apiAuthenticationDomains);

                result.put(entry.getKey(), Collections.unmodifiableList(apiAuthenticationDomains));
            }
        }

        if (!result.containsKey("default")) {
            List<AuthenticationDomain<ApiAuthenticationFrontend>> apiAuthenticationDomains = new ArrayList<>(globalApiAuthenticationDomains);
            Collections.sort(apiAuthenticationDomains);

            result.put("default", Collections.unmodifiableList(apiAuthenticationDomains));
        }
        
        return Collections.unmodifiableMap(result);
    }
    
    private void destroyDestroyables(List<Destroyable> destroyableComponents) {
        for (Destroyable destroyable : destroyableComponents) {
            try {
                destroyable.destroy();
            } catch (Exception e) {
                log.error("Error while destroying " + destroyable, e);
            }
        }
    }

    private void createAuthFailureListeners(List<AuthFailureListener> ipAuthFailureListeners,
            Multimap<String, AuthFailureListener> authBackendFailureListeners, List<ClientBlockRegistry<InetAddress>> ipClientBlockRegistries,
            Multimap<String, ClientBlockRegistry<String>> authBackendUserClientBlockRegistries, List<Destroyable> destroyableComponents0) {

        for (Entry<String, ConfigV7.AuthFailureListener> entry : config.dynamic.auth_failure_listeners.getListeners().entrySet()) {
            try {
                Settings entrySettings = Settings.builder().put(esSettings)
                        .put(Settings.builder().loadFromSource(entry.getValue().asJson(), XContentType.JSON).build()).build();

                String type = entry.getValue().type;
                String authenticationBackend = entry.getValue().authentication_backend;

                AuthFailureListener authFailureListener = modulesRegistry.getAuthFailureListeners().getInstance(type, entrySettings, configPath);

                if (Strings.isNullOrEmpty(authenticationBackend)) {
                    ipAuthFailureListeners.add(authFailureListener);

                    if (authFailureListener instanceof ClientBlockRegistry) {
                        if (InetAddress.class.isAssignableFrom(((ClientBlockRegistry<?>) authFailureListener).getClientIdType())) {
                            @SuppressWarnings("unchecked")
                            ClientBlockRegistry<InetAddress> clientBlockRegistry = (ClientBlockRegistry<InetAddress>) authFailureListener;

                            ipClientBlockRegistries.add(clientBlockRegistry);
                        } else {
                            log.error("Illegal ClientIdType for AuthFailureListener" + entry.getKey() + ": "
                                    + ((ClientBlockRegistry<?>) authFailureListener).getClientIdType() + "; must be InetAddress.");
                        }
                    }

                } else {

                    authenticationBackend = modulesRegistry.getAuthenticationBackends().getClassName(authenticationBackend);

                    authBackendFailureListeners.put(authenticationBackend, authFailureListener);

                    if (authFailureListener instanceof ClientBlockRegistry) {
                        if (String.class.isAssignableFrom(((ClientBlockRegistry<?>) authFailureListener).getClientIdType())) {
                            @SuppressWarnings("unchecked")
                            ClientBlockRegistry<String> clientBlockRegistry = (ClientBlockRegistry<String>) authFailureListener;

                            authBackendUserClientBlockRegistries.put(authenticationBackend, clientBlockRegistry);
                        } else {
                            log.error("Illegal ClientIdType for AuthFailureListener" + entry.getKey() + ": "
                                    + ((ClientBlockRegistry<?>) authFailureListener).getClientIdType() + "; must be InetAddress.");
                        }
                    }
                }

                if (authFailureListener instanceof Destroyable) {
                    destroyableComponents0.add((Destroyable) authFailureListener);
                }
            } catch (Exception e) {
                log.error("Error while creating " + entry, e);
            }
        }

    }
    
    @Override
    public Map<String, Object> getAuthTokenProviderConfig() {
        return config.dynamic.auth_token_provider;
    }
    
    public ComponentState getComponentState() {
        return componentState;
    }
    
    @Override
    public SgDynamicConfiguration<FrontendConfig> getFrontendConfig() {
        return frontendConfig;
    }
}
