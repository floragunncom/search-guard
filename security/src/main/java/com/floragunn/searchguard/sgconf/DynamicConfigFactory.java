package com.floragunn.searchguard.sgconf;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.searchguard.auth.AuthenticationDomain;
import com.floragunn.searchguard.auth.HTTPAuthenticator;
import com.floragunn.searchguard.auth.internal.InternalAuthenticationBackend;
import com.floragunn.searchguard.configuration.ClusterInfoHolder;
import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.configuration.ConfigurationChangeListener;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.internal_users.InternalUser;
import com.floragunn.searchguard.configuration.variables.ConfigVarService;
import com.floragunn.searchguard.modules.SearchGuardModulesRegistry;
import com.floragunn.searchguard.modules.state.ComponentState;
import com.floragunn.searchguard.modules.state.ComponentStateProvider;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.sgconf.impl.v7.ActionGroupsV7;
import com.floragunn.searchguard.sgconf.impl.v7.BlocksV7;
import com.floragunn.searchguard.sgconf.impl.v7.ConfigV7;
import com.floragunn.searchguard.sgconf.impl.v7.FrontendConfig;
import com.floragunn.searchguard.sgconf.impl.v7.RoleMappingsV7;
import com.floragunn.searchguard.sgconf.impl.v7.RoleV7;
import com.floragunn.searchguard.sgconf.impl.v7.TenantV7;
import com.floragunn.searchguard.sgconf.internal_users_db.InternalUsersDatabase;


public class DynamicConfigFactory implements Initializable, ConfigurationChangeListener, ComponentStateProvider {

    protected final Logger log = LogManager.getLogger(this.getClass());
    private final ConfigurationRepository cr;
    private final AtomicBoolean initialized = new AtomicBoolean();
    private final List<DCFListener> listeners = new ArrayList<>();
    private final Settings esSettings;
    private final Path configPath;
    private final List<Consumer<SgDynamicConfiguration<ConfigV7>>> configChangeConsumers = new ArrayList<>();
    private final SearchGuardModulesRegistry modulesRegistry;
    private final InternalUsersDatabase internalUsersDatabase;
    private final StaticSgConfig staticSgConfig;
    private final ComponentState componentState = new ComponentState(2, null, "dynamic_config", DynamicConfigFactory.class);
    private volatile ConfigMap currentConfig;
    private final List<Supplier<List<AuthenticationDomain<HTTPAuthenticator>>>> authenticationDomainInjectors = new ArrayList<>();

    SgDynamicConfiguration<?> config;
    
    public DynamicConfigFactory(ConfigurationRepository cr, StaticSgConfig staticSgConfig, final Settings esSettings, 
            final Path configPath, Client client, ThreadPool threadPool, ClusterInfoHolder cih, SearchGuardModulesRegistry modulesRegistry,
            ConfigVarService configVarService) {
        super();
        this.cr = cr;
        this.esSettings = esSettings;
        this.configPath = configPath;
        this.modulesRegistry = modulesRegistry;
        this.staticSgConfig = staticSgConfig;
        
        internalUsersDatabase = new InternalUsersDatabase(this);
        
        InternalAuthenticationBackend.Factory internalAuthBackendFactory = new InternalAuthenticationBackend.Factory(internalUsersDatabase);
        
        modulesRegistry.getAuthenticationBackends().add(Arrays.asList("intern", "internal", InternalAuthenticationBackend.class.getName()),
                internalAuthBackendFactory);
        modulesRegistry.getAuthorizationBackends().add(Arrays.asList("intern", "internal", InternalAuthenticationBackend.class.getName()),
                internalAuthBackendFactory);
        this.cr.subscribeOnChange(this);
        
        configVarService.addChangeListener(() -> {
            if (currentConfig != null) {
                onChange(this.currentConfig);
            }
        });
    }
    
    @Override
    public void onChange(ConfigMap configMap) {
        this.currentConfig = configMap;
        SgDynamicConfiguration<ActionGroupsV7> actionGroups = configMap.get(CType.ACTIONGROUPS);
        config = cr.getConfiguration(CType.CONFIG);
        SgDynamicConfiguration<InternalUser> internalusers = configMap.get(CType.INTERNALUSERS);
        SgDynamicConfiguration<RoleV7> roles = configMap.get(CType.ROLES);
        SgDynamicConfiguration<RoleMappingsV7> rolesmapping = configMap.get(CType.ROLESMAPPING);
        SgDynamicConfiguration<TenantV7> tenants = configMap.get(CType.TENANTS);
        SgDynamicConfiguration<BlocksV7> blocks = configMap.get(CType.BLOCKS);
        SgDynamicConfiguration<FrontendConfig> frontendConfig = configMap.get(CType.FRONTEND_CONFIG);
        
        if(log.isDebugEnabled()) {
            String logmsg = "current config\n"+
            " actionGroups: "+actionGroups.getImplementingClass()+" with "+actionGroups.getCEntries().size()+" entries\n"+
            " config: "+config.getImplementingClass()+" with "+config.getCEntries().size()+" entries\n"+
            " internalusers: "+internalusers.getImplementingClass()+" with "+internalusers.getCEntries().size()+" entries\n"+
            " roles: "+roles.getImplementingClass()+" with "+roles.getCEntries().size()+" entries\n"+
            " rolesmapping: "+rolesmapping.getImplementingClass()+" with "+rolesmapping.getCEntries().size()+" entries\n"+
            " tenants: "+tenants.getImplementingClass()+" with "+tenants.getCEntries().size()+" entries \n"+
            " blocks: "+blocks.getImplementingClass()+" with "+blocks.getCEntries().size()+" entries \n";
            log.debug(logmsg);
        }

        staticSgConfig.addTo(roles);
        staticSgConfig.addTo(actionGroups);
        staticSgConfig.addTo(tenants);

        log.debug("Static configuration loaded (total roles: {}/total action groups: {}/total tenants: {})", roles.getCEntries().size(),
                actionGroups.getCEntries().size(), tenants.getCEntries().size());

        //rebuild v7 Models
        DynamicConfigModel dcm = new DynamicConfigModelV7(getConfigV7(config), frontendConfig, esSettings, configPath, modulesRegistry, authenticationDomainInjectors, null);
        InternalUsersModel ium = new InternalUsersModelV7((SgDynamicConfiguration<InternalUser>) internalusers);
        ConfigModel cm = new ConfigModelV7((SgDynamicConfiguration<RoleV7>) roles, (SgDynamicConfiguration<RoleMappingsV7>) rolesmapping,
                (SgDynamicConfiguration<ActionGroupsV7>) actionGroups, (SgDynamicConfiguration<TenantV7>) tenants,
                (SgDynamicConfiguration<BlocksV7>) blocks, dcm, esSettings);

        componentState.replacePart(dcm.getComponentState());
        dcm.getComponentState().setConfigVersion(config.getDocVersion());

        //notify listeners

        for (DCFListener listener : listeners) {
            if (log.isTraceEnabled()) {
                log.trace("Notifying DCFListener '{}' about configuration changes");
            }
            listener.onChanged(cm, dcm, ium);
        }

        notifyConfigChangeListeners(config);

        initialized.set(true);
    }
    
    
    public String getLicenseString() {

        if (!isInitialized()) {
            throw new RuntimeException("Can not retrieve license because not initialized (yet)");
        }

        @SuppressWarnings("unchecked")
        SgDynamicConfiguration<ConfigV7> c = (SgDynamicConfiguration<ConfigV7>) config;
        return c.getCEntry("sg_config").dynamic.license;

    }
     
    private static ConfigV7 getConfigV7(SgDynamicConfiguration<?> sdc) {
        @SuppressWarnings("unchecked")
        SgDynamicConfiguration<ConfigV7> c = (SgDynamicConfiguration<ConfigV7>) sdc;
        return c.getCEntry("sg_config");
    }
    
    @Override
    public final boolean isInitialized() {
        return initialized.get();
    }
    

    @SuppressWarnings("unchecked")
    private void notifyConfigChangeListeners(SgDynamicConfiguration<?> config) {
        for (Consumer<SgDynamicConfiguration<ConfigV7>> consumer : this.configChangeConsumers) {
            try {
                consumer.accept((SgDynamicConfiguration<ConfigV7>) config);
            } catch (Exception e) {
                log.error("Error in " + consumer + " consuming " + config);
            }
        }
    }
    
    public <T> void addConfigChangeListener(Class<T> configType, Consumer<SgDynamicConfiguration<T>> listener) {
        if (configType.equals(ConfigV7.class)) {
            @SuppressWarnings("rawtypes")
            Consumer rawListener = listener;
            @SuppressWarnings("unchecked")
            Consumer<SgDynamicConfiguration<ConfigV7>> configListener = (Consumer<SgDynamicConfiguration<ConfigV7>>) rawListener;
            configChangeConsumers.add(configListener);
        } else {
            throw new RuntimeException(configType + " is not supported by addConfigChangeListener()");
        }
    }
    
    public void registerDCFListener(DCFListener listener) {
        listeners.add(listener);
    }
    
    public interface DCFListener {
        void onChanged(ConfigModel cm, DynamicConfigModel dcm, InternalUsersModel ium);
    }
    
    private static class InternalUsersModelV7 extends InternalUsersModel {
        
        SgDynamicConfiguration<InternalUser> configuration;
        
        public InternalUsersModelV7(SgDynamicConfiguration<InternalUser> configuration) {
            super();
            this.configuration = configuration;
        }

        @Override
        public boolean exists(String user) {
            return configuration.exists(user);
        }

        @Override
        public List<String> getBackenRoles(String user) {
            InternalUser tmp = configuration.getCEntry(user);
            return tmp==null?null:tmp.getBackendRoles();
        }

        @Override
        public Map<String, Object> getAttributes(String user) {
            InternalUser tmp = configuration.getCEntry(user);
            return tmp==null?null:tmp.getAttributes();
        }

        @Override
        public String getDescription(String user) {
            InternalUser tmp = configuration.getCEntry(user);
            return tmp==null?null:tmp.getDescription();
        }

        @Override
        public String getHash(String user) {
            InternalUser tmp = configuration.getCEntry(user);
            return tmp==null?null:tmp.getPasswordHash();
        }
        
        public List<String> getSearchGuardRoles(String user) {
            InternalUser tmp = configuration.getCEntry(user);
            return tmp==null?Collections.emptyList():tmp.getSearchGuardRoles();
        }
        
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }
 
    public void addAuthenticationDomainInjector(Supplier<List<AuthenticationDomain<HTTPAuthenticator>>> injector) {
        this.authenticationDomainInjectors.add(injector);
    }
    
}
