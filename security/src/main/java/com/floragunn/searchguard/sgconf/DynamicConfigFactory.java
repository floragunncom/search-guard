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
import com.floragunn.searchguard.configuration.ConfigurationChangeListener;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.secrets.SecretsService;
import com.floragunn.searchguard.modules.SearchGuardModulesRegistry;
import com.floragunn.searchguard.modules.state.ComponentState;
import com.floragunn.searchguard.modules.state.ComponentStateProvider;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.sgconf.impl.v6.ActionGroupsV6;
import com.floragunn.searchguard.sgconf.impl.v6.ConfigV6;
import com.floragunn.searchguard.sgconf.impl.v6.InternalUserV6;
import com.floragunn.searchguard.sgconf.impl.v6.RoleMappingsV6;
import com.floragunn.searchguard.sgconf.impl.v6.RoleV6;
import com.floragunn.searchguard.sgconf.impl.v7.ActionGroupsV7;
import com.floragunn.searchguard.sgconf.impl.v7.BlocksV7;
import com.floragunn.searchguard.sgconf.impl.v7.ConfigV7;
import com.floragunn.searchguard.sgconf.impl.v7.FrontendConfig;
import com.floragunn.searchguard.sgconf.impl.v7.InternalUserV7;
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
    private final List<Supplier<List<AuthenticationDomain<HTTPAuthenticator>>>> authenticationDomainInjectors = new ArrayList<>();

    SgDynamicConfiguration<?> config;
    
    public DynamicConfigFactory(ConfigurationRepository cr, StaticSgConfig staticSgConfig, final Settings esSettings, 
            final Path configPath, Client client, ThreadPool threadPool, ClusterInfoHolder cih, SearchGuardModulesRegistry modulesRegistry,
            SecretsService secretsStorageService) {
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
        
        secretsStorageService.addChangeListener(() -> {
            if (config != null) {
                onChange(Collections.emptyMap());
            }
        });
    }
    
    @Override
    public void onChange(Map<CType, SgDynamicConfiguration<?>> typeToConfig) {
        SgDynamicConfiguration<?> actionGroups = cr.getConfiguration(CType.ACTIONGROUPS);
        config = cr.getConfiguration(CType.CONFIG);
        SgDynamicConfiguration<?> internalusers = cr.getConfiguration(CType.INTERNALUSERS);
        SgDynamicConfiguration<?> roles = cr.getConfiguration(CType.ROLES);
        SgDynamicConfiguration<?> rolesmapping = cr.getConfiguration(CType.ROLESMAPPING);
        SgDynamicConfiguration<?> tenants = cr.getConfiguration(CType.TENANTS);
        SgDynamicConfiguration<?> blocks = cr.getConfiguration(CType.BLOCKS);
        @SuppressWarnings("unchecked")
        SgDynamicConfiguration<FrontendConfig> frontendConfig = (SgDynamicConfiguration<FrontendConfig>) cr.getConfiguration(CType.FRONTEND_CONFIG);
        
        if(log.isDebugEnabled()) {
            String logmsg = "current config (because of "+typeToConfig.keySet()+")\n"+
            " actionGroups: "+actionGroups.getImplementingClass()+" with "+actionGroups.getCEntries().size()+" entries\n"+
            " config: "+config.getImplementingClass()+" with "+config.getCEntries().size()+" entries\n"+
            " internalusers: "+internalusers.getImplementingClass()+" with "+internalusers.getCEntries().size()+" entries\n"+
            " roles: "+roles.getImplementingClass()+" with "+roles.getCEntries().size()+" entries\n"+
            " rolesmapping: "+rolesmapping.getImplementingClass()+" with "+rolesmapping.getCEntries().size()+" entries\n"+
            " tenants: "+tenants.getImplementingClass()+" with "+tenants.getCEntries().size()+" entries \n"+
            " blocks: "+blocks.getImplementingClass()+" with "+blocks.getCEntries().size()+" entries \n";
            log.debug(logmsg);
        }

        if(config.getImplementingClass() == ConfigV7.class) {
            staticSgConfig.addTo(roles);
            staticSgConfig.addTo(actionGroups);
            staticSgConfig.addTo(tenants);

            log.debug("Static configuration loaded (total roles: {}/total action groups: {}/total tenants: {})", roles.getCEntries().size(),
                    actionGroups.getCEntries().size(), tenants.getCEntries().size());

            notifyConfigChangeListeners(config);

            //rebuild v7 Models
            DynamicConfigModel dcm = new DynamicConfigModelV7(getConfigV7(config), frontendConfig, esSettings, configPath, modulesRegistry, authenticationDomainInjectors, null);
            InternalUsersModel ium = new InternalUsersModelV7((SgDynamicConfiguration<InternalUserV7>) internalusers);
            ConfigModel cm = new ConfigModelV7((SgDynamicConfiguration<RoleV7>) roles,(SgDynamicConfiguration<RoleMappingsV7>)rolesmapping,
                    (SgDynamicConfiguration<ActionGroupsV7>)actionGroups, (SgDynamicConfiguration<TenantV7>) tenants, (SgDynamicConfiguration<BlocksV7>) blocks, dcm, esSettings);

            
            componentState.replacePart(dcm.getComponentState());
            dcm.getComponentState().setConfigVersion(config.getDocVersion());
            
            //notify listeners
            
            for(DCFListener listener: listeners) {
            	if (log.isTraceEnabled()) {
            		log.trace("Notifying DCFListener '{}' about configuration changes" );	
            	}            	
                listener.onChanged(cm, dcm, ium);
            }                    
        } else {
            //rebuild v6 Models
            @SuppressWarnings("deprecation")
            DynamicConfigModel dcmv6 = new DynamicConfigModelV6(getConfigV6(config), esSettings, configPath, modulesRegistry);
            InternalUsersModel iumv6 = new InternalUsersModelV6((SgDynamicConfiguration<InternalUserV6>) internalusers);
            ConfigModel cmv6 = new ConfigModelV6((SgDynamicConfiguration<RoleV6>) roles, (SgDynamicConfiguration<ActionGroupsV6>)actionGroups, (SgDynamicConfiguration<RoleMappingsV6>)rolesmapping, dcmv6, esSettings);
            
            componentState.replacePart(dcmv6.getComponentState());

            //notify listeners
            
            for(DCFListener listener: listeners) {
                listener.onChanged(cmv6, dcmv6, iumv6);
            }
        }

        initialized.set(true);
        
    }
    
    
    public String getLicenseString() {
        
        if(!isInitialized()) {
            throw new RuntimeException("Can not retrieve license because not initialized (yet)");
        }
        
        if(config.getImplementingClass() == ConfigV6.class) {
            @SuppressWarnings("unchecked")
            SgDynamicConfiguration<ConfigV6> c = (SgDynamicConfiguration<ConfigV6>) config;
            return c.getCEntry("searchguard").dynamic.license;
        } else {
            @SuppressWarnings("unchecked")
            SgDynamicConfiguration<ConfigV7> c = (SgDynamicConfiguration<ConfigV7>) config;
            return c.getCEntry("sg_config").dynamic.license;
        }
    }
    
    private static ConfigV6 getConfigV6(SgDynamicConfiguration<?> sdc) {
        @SuppressWarnings("unchecked")
        SgDynamicConfiguration<ConfigV6> c = (SgDynamicConfiguration<ConfigV6>) sdc;
        return c.getCEntry("searchguard");
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
    
    public void addAuthenticationDomainInjector(Supplier<List<AuthenticationDomain<HTTPAuthenticator>>> injector) {
        this.authenticationDomainInjectors.add(injector);
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
        
        SgDynamicConfiguration<InternalUserV7> configuration;
        
        public InternalUsersModelV7(SgDynamicConfiguration<InternalUserV7> configuration) {
            super();
            this.configuration = configuration;
        }

        @Override
        public boolean exists(String user) {
            return configuration.exists(user);
        }

        @Override
        public List<String> getBackenRoles(String user) {
            InternalUserV7 tmp = configuration.getCEntry(user);
            return tmp==null?null:tmp.getBackend_roles();
        }

        @Override
        public Map<String, Object> getAttributes(String user) {
            InternalUserV7 tmp = configuration.getCEntry(user);
            return tmp==null?null:tmp.getAttributes();
        }

        @Override
        public String getDescription(String user) {
            InternalUserV7 tmp = configuration.getCEntry(user);
            return tmp==null?null:tmp.getDescription();
        }

        @Override
        public String getHash(String user) {
            InternalUserV7 tmp = configuration.getCEntry(user);
            return tmp==null?null:tmp.getHash();
        }
        
        public List<String> getSearchGuardRoles(String user) {
            InternalUserV7 tmp = configuration.getCEntry(user);
            return tmp==null?Collections.emptyList():tmp.getSearch_guard_roles();
        }
        
    }
    
    private static class InternalUsersModelV6 extends InternalUsersModel {
        SgDynamicConfiguration<InternalUserV6> configuration;

        public InternalUsersModelV6(SgDynamicConfiguration<InternalUserV6> configuration) {
            super();
            this.configuration = configuration;
        }

        @Override
        public boolean exists(String user) {
            return configuration.exists(user);
        }

        @Override
        public List<String> getBackenRoles(String user) {
            InternalUserV6 tmp = configuration.getCEntry(user);
            return tmp==null?null:tmp.getRoles();
        }

        @Override
        public Map<String, Object> getAttributes(String user) {
            InternalUserV6 tmp = configuration.getCEntry(user);
            return tmp==null?null:tmp.getAttributes();
        }

        @Override
        public String getDescription(String user) {
            return null;
        }

        @Override
        public String getHash(String user) {
            InternalUserV6 tmp = configuration.getCEntry(user);
            return tmp==null?null:tmp.getHash();
        }
        
        public List<String> getSearchGuardRoles(String user) {
            return Collections.emptyList();
        }
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }
   
}
