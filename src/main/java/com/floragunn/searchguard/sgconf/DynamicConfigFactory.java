package com.floragunn.searchguard.sgconf;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchguard.auth.internal.InternalAuthenticationBackend;
import com.floragunn.searchguard.configuration.ClusterInfoHolder;
import com.floragunn.searchguard.configuration.ConfigurationChangeListener;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.StaticResourceException;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.sgconf.impl.v6.ActionGroupsV6;
import com.floragunn.searchguard.sgconf.impl.v6.ConfigV6;
import com.floragunn.searchguard.sgconf.impl.v6.InternalUserV6;
import com.floragunn.searchguard.sgconf.impl.v6.RoleMappingsV6;
import com.floragunn.searchguard.sgconf.impl.v6.RoleV6;
import com.floragunn.searchguard.sgconf.impl.v7.ActionGroupsV7;
import com.floragunn.searchguard.sgconf.impl.v7.ConfigV7;
import com.floragunn.searchguard.sgconf.impl.v7.InternalUserV7;
import com.floragunn.searchguard.sgconf.impl.v7.RoleMappingsV7;
import com.floragunn.searchguard.sgconf.impl.v7.RoleV7;
import com.floragunn.searchguard.sgconf.impl.v7.TenantV7;
import com.floragunn.searchguard.support.ConfigConstants;

public class DynamicConfigFactory implements Initializable, ConfigurationChangeListener {
    
    private static SgDynamicConfiguration<RoleV7> staticRoles = SgDynamicConfiguration.empty();
    private static SgDynamicConfiguration<ActionGroupsV7> staticActionGroups = SgDynamicConfiguration.empty();
    private static SgDynamicConfiguration<TenantV7> staticTenants = SgDynamicConfiguration.empty();
    
    //only for unittesting
    static void resetStatics() {
        staticRoles = SgDynamicConfiguration.empty();
        staticActionGroups = SgDynamicConfiguration.empty();
        staticTenants = SgDynamicConfiguration.empty();
    }
    
    private void loadStaticConfig() throws IOException {
        JsonNode staticRolesJsonNode = DefaultObjectMapper.YAML_MAPPER
                .readTree(DynamicConfigFactory.class.getResourceAsStream("/static_config/static_roles.yml"));
        staticRoles = SgDynamicConfiguration.fromNode(staticRolesJsonNode, CType.ROLES, 2, 0, 0);

        JsonNode staticActionGroupsJsonNode = DefaultObjectMapper.YAML_MAPPER
                .readTree(DynamicConfigFactory.class.getResourceAsStream("/static_config/static_action_groups.yml"));
        staticActionGroups = SgDynamicConfiguration.fromNode(staticActionGroupsJsonNode, CType.ACTIONGROUPS, 2, 0, 0);

        JsonNode staticTenantsJsonNode = DefaultObjectMapper.YAML_MAPPER
                .readTree(DynamicConfigFactory.class.getResourceAsStream("/static_config/static_tenants.yml"));
        staticTenants = SgDynamicConfiguration.fromNode(staticTenantsJsonNode, CType.TENANTS, 2, 0, 0);
    }
    
    public final static SgDynamicConfiguration<?> addStatics(SgDynamicConfiguration<?> original) {
        if(original.getCType() == CType.ACTIONGROUPS && !staticActionGroups.getCEntries().isEmpty()) {
            original.add(staticActionGroups.deepClone());
        }
        
        if(original.getCType() == CType.ROLES && !staticRoles.getCEntries().isEmpty()) {
            original.add(staticRoles.deepClone());
        }
        
        if(original.getCType() == CType.TENANTS && !staticTenants.getCEntries().isEmpty()) {
            original.add(staticTenants.deepClone());
        }
        
        return original;
    }
    
    protected final Logger log = LogManager.getLogger(this.getClass());
    private final ConfigurationRepository cr;
    private final AtomicBoolean initialized = new AtomicBoolean();
    private final List<DCFListener> listeners = new ArrayList<>();
    private final Settings esSettings;
    private final Path configPath;
    private final InternalAuthenticationBackend iab = new InternalAuthenticationBackend();

    SgDynamicConfiguration<?> config;
    
    public DynamicConfigFactory(ConfigurationRepository cr, final Settings esSettings, 
            final Path configPath, Client client, ThreadPool threadPool, ClusterInfoHolder cih) {
        super();
        this.cr = cr;
        this.esSettings = esSettings;
        this.configPath = configPath;
        
        if(esSettings.getAsBoolean(ConfigConstants.SEARCHGUARD_UNSUPPORTED_LOAD_STATIC_RESOURCES, true)) {
            try {
                loadStaticConfig();
            } catch (IOException e) {
                throw new StaticResourceException("Unable to load static resources due to "+e, e);
            }
        } else {
            log.info("Static resources will not be loaded.");
        }
        
        registerDCFListener(this.iab);
        this.cr.subscribeOnChange(this);
    }
    
    @Override
    public void onChange(Map<CType, SgDynamicConfiguration<?>> typeToConfig) {

        SgDynamicConfiguration<?> actionGroups = cr.getConfiguration(CType.ACTIONGROUPS);
        config = cr.getConfiguration(CType.CONFIG);
        SgDynamicConfiguration<?> internalusers = cr.getConfiguration(CType.INTERNALUSERS);
        SgDynamicConfiguration<?> roles = cr.getConfiguration(CType.ROLES);
        SgDynamicConfiguration<?> rolesmapping = cr.getConfiguration(CType.ROLESMAPPING);
        SgDynamicConfiguration<?> tenants = cr.getConfiguration(CType.TENANTS);
        
        if(log.isDebugEnabled()) {
            String logmsg = "current config (because of "+typeToConfig.keySet()+")\n"+
            " actionGroups: "+actionGroups.getImplementingClass()+" with "+actionGroups.getCEntries().size()+" entries\n"+
            " config: "+config.getImplementingClass()+" with "+config.getCEntries().size()+" entries\n"+
            " internalusers: "+internalusers.getImplementingClass()+" with "+internalusers.getCEntries().size()+" entries\n"+
            " roles: "+roles.getImplementingClass()+" with "+roles.getCEntries().size()+" entries\n"+
            " rolesmapping: "+rolesmapping.getImplementingClass()+" with "+rolesmapping.getCEntries().size()+" entries\n"+
            " tenants: "+tenants.getImplementingClass()+" with "+tenants.getCEntries().size()+" entries";
            log.debug(logmsg);
            
        }

        if(config.getImplementingClass() == ConfigV7.class) {
                //statics
                
                if(roles.containsAny(staticRoles)) {
                    throw new StaticResourceException("Cannot override static roles");
                }
                if(!roles.add(staticRoles) && !staticRoles.getCEntries().isEmpty()) {
                    throw new StaticResourceException("Unable to load static roles");
                }

                log.debug("Static roles loaded ({})", staticRoles.getCEntries().size());

                if(actionGroups.containsAny(staticActionGroups)) {
                    throw new StaticResourceException("Cannot override static action groups");
                }
                if(!actionGroups.add(staticActionGroups) && !staticActionGroups.getCEntries().isEmpty()) {
                    throw new StaticResourceException("Unable to load static action groups");
                }
                

                log.debug("Static action groups loaded ({})", staticActionGroups.getCEntries().size());
                
                if(tenants.containsAny(staticTenants)) {
                    throw new StaticResourceException("Cannot override static tenants");
                }
                if(!tenants.add(staticTenants) && !staticTenants.getCEntries().isEmpty()) {
                    throw new StaticResourceException("Unable to load static tenants");
                }
                

                log.debug("Static tenants loaded ({})", staticTenants.getCEntries().size());

                log.debug("Static configuration loaded (total roles: {}/total action groups: {}/total tenants: {})", roles.getCEntries().size(), actionGroups.getCEntries().size(), tenants.getCEntries().size());

            

            //rebuild v7 Models
            DynamicConfigModel dcm = new DynamicConfigModelV7(getConfigV7(config), esSettings, configPath, iab);
            InternalUsersModel ium = new InternalUsersModelV7((SgDynamicConfiguration<InternalUserV7>) internalusers);
            ConfigModel cm = new ConfigModelV7((SgDynamicConfiguration<RoleV7>) roles,(SgDynamicConfiguration<RoleMappingsV7>)rolesmapping, (SgDynamicConfiguration<ActionGroupsV7>)actionGroups, (SgDynamicConfiguration<TenantV7>) tenants,dcm, esSettings);

            //notify listeners
            
            for(DCFListener listener: listeners) {
                listener.onChanged(cm, dcm, ium);
            }
        
        } else {

            //rebuild v6 Models
            DynamicConfigModel dcmv6 = new DynamicConfigModelV6(getConfigV6(config), esSettings, configPath, iab);
            InternalUsersModel iumv6 = new InternalUsersModelV6((SgDynamicConfiguration<InternalUserV6>) internalusers);
            ConfigModel cmv6 = new ConfigModelV6((SgDynamicConfiguration<RoleV6>) roles, (SgDynamicConfiguration<ActionGroupsV6>)actionGroups, (SgDynamicConfiguration<RoleMappingsV6>)rolesmapping, dcmv6, esSettings);
            
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
            SgDynamicConfiguration<ConfigV6> c = (SgDynamicConfiguration<ConfigV6>) config;
            return c.getCEntry("searchguard").dynamic.license;
        } else {
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
    
    public void registerDCFListener(DCFListener listener) {
        listeners.add(listener);
    }
    
    public static interface DCFListener {
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
        public Map<String, String> getAttributes(String user) {
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
        public Map<String, String> getAttributes(String user) {
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
   
}
