package com.floragunn.searchguard.sgconf;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.searchguard.SearchGuardModulesRegistry;
import com.floragunn.searchguard.authz.RoleMapping;
import com.floragunn.searchguard.configuration.ClusterInfoHolder;
import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.configuration.ConfigurationChangeListener;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.variables.ConfigVarService;
import com.floragunn.searchguard.modules.state.ComponentState;
import com.floragunn.searchguard.modules.state.ComponentStateProvider;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.sgconf.impl.v7.ActionGroupsV7;
import com.floragunn.searchguard.sgconf.impl.v7.BlocksV7;
import com.floragunn.searchguard.sgconf.impl.v7.RoleV7;
import com.floragunn.searchguard.sgconf.impl.v7.TenantV7;


public class DynamicConfigFactory implements Initializable, ConfigurationChangeListener, ComponentStateProvider {

    protected final Logger log = LogManager.getLogger(this.getClass());
    private final ConfigurationRepository cr;
    private final AtomicBoolean initialized = new AtomicBoolean();
    private final List<DCFListener> listeners = new ArrayList<>();
    private final Settings esSettings;
    private final ComponentState componentState = new ComponentState(2, null, "dynamic_config", DynamicConfigFactory.class);

    SgDynamicConfiguration<?> config;

    public DynamicConfigFactory(ConfigurationRepository cr, final Settings esSettings, final Path configPath, Client client, ThreadPool threadPool,
            ClusterInfoHolder cih, SearchGuardModulesRegistry modulesRegistry, ConfigVarService configVarService) {
        super();
        this.cr = cr;
        this.esSettings = esSettings;

        this.cr.subscribeOnChange(this);

    }

    @Override
    public void onChange(ConfigMap configMap) {
        SgDynamicConfiguration<ActionGroupsV7> actionGroups = configMap.get(CType.ACTIONGROUPS);
        config = cr.getConfiguration(CType.CONFIG);
        SgDynamicConfiguration<RoleV7> roles = configMap.get(CType.ROLES);
        SgDynamicConfiguration<RoleMapping> rolesmapping = configMap.get(CType.ROLESMAPPING);
        SgDynamicConfiguration<TenantV7> tenants = configMap.get(CType.TENANTS);
        SgDynamicConfiguration<BlocksV7> blocks = configMap.get(CType.BLOCKS);
        
        if(log.isDebugEnabled()) {
            String logmsg = "current config\n"+
            " actionGroups: "+actionGroups.getImplementingClass()+" with "+actionGroups.getCEntries().size()+" entries\n"+
            " config: "+config.getImplementingClass()+" with "+config.getCEntries().size()+" entries\n"+
            " roles: "+roles.getImplementingClass()+" with "+roles.getCEntries().size()+" entries\n"+
            " rolesmapping: "+rolesmapping.getImplementingClass()+" with "+rolesmapping.getCEntries().size()+" entries\n"+
            " tenants: "+tenants.getImplementingClass()+" with "+tenants.getCEntries().size()+" entries \n"+
            " blocks: "+blocks.getImplementingClass()+" with "+blocks.getCEntries().size()+" entries \n";
            log.debug(logmsg);
        }

        //rebuild v7 Models
        ConfigModel cm = new ConfigModelV7((SgDynamicConfiguration<RoleV7>) roles, rolesmapping,
                (SgDynamicConfiguration<ActionGroupsV7>) actionGroups, (SgDynamicConfiguration<TenantV7>) tenants,
                (SgDynamicConfiguration<BlocksV7>) blocks, esSettings);

        //notify listeners

        for (DCFListener listener : listeners) {
            if (log.isTraceEnabled()) {
                log.trace("Notifying DCFListener '{}' about configuration changes");
            }
            listener.onChanged(cm);
        }

        initialized.set(true);
    }

    @Override
    public final boolean isInitialized() {
        return initialized.get();
    }
    
    public void registerDCFListener(DCFListener listener) {
        listeners.add(listener);
    }
    
    public interface DCFListener {
        void onChanged(ConfigModel cm);
    }
    
    @Override
    public ComponentState getComponentState() {
        return componentState;
    }
 
    
}
