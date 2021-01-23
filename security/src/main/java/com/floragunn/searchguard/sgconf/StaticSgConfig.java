package com.floragunn.searchguard.sgconf;

import java.util.HashSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.sgconf.impl.v7.ActionGroupsV7;
import com.floragunn.searchguard.sgconf.impl.v7.RoleV7;
import com.floragunn.searchguard.sgconf.impl.v7.TenantV7;
import com.floragunn.searchguard.support.ConfigConstants;

public class StaticSgConfig {
    private static final Logger log = LogManager.getLogger(StaticSgConfig.class);

    private final SgDynamicConfiguration<RoleV7> staticRoles;
    private final SgDynamicConfiguration<ActionGroupsV7> staticActionGroups;
    private final SgDynamicConfiguration<TenantV7> staticTenants;

    public StaticSgConfig(Settings settings) {
        if (settings.getAsBoolean(ConfigConstants.SEARCHGUARD_UNSUPPORTED_LOAD_STATIC_RESOURCES, true)) {
            staticRoles = readConfig("/static_config/static_roles.yml", RoleV7.class);
            staticActionGroups = readConfig("/static_config/static_action_groups.yml", ActionGroupsV7.class);
            staticTenants = readConfig("/static_config/static_tenants.yml", TenantV7.class);
        } else {
            log.info("searchguard.unsupported.load_static_resources is set to false. Static resources will not be loaded.");
            staticRoles = SgDynamicConfiguration.empty();
            staticActionGroups = SgDynamicConfiguration.empty();
            staticTenants = SgDynamicConfiguration.empty();
        }
    }

    public SgDynamicConfiguration<?> addTo(SgDynamicConfiguration<?> original) {
        SgDynamicConfiguration<?> staticConfig = get(original);
        
        if (staticConfig.getCEntries().isEmpty()) {
            return original;
        }
        
        checkForOverriddenEntries(original, staticConfig);
        
        original.add(staticConfig.deepClone());

        if (log.isDebugEnabled()) {
            log.debug(staticConfig.getCEntries().size() + " static " + original.getCType().toLCString() + " loaded");
        }
        
        return original;
    }

    @SuppressWarnings("unchecked")
    public <ConfigType> SgDynamicConfiguration<ConfigType> get(SgDynamicConfiguration<ConfigType> original) {
        if (original.getVersion() != 2) {
            return SgDynamicConfiguration.empty();
        }

        switch (original.getCType()) {
        case ACTIONGROUPS:
            return (SgDynamicConfiguration<ConfigType>) staticActionGroups;
        case ROLES:
            return (SgDynamicConfiguration<ConfigType>) staticRoles;
        case TENANTS:
            return (SgDynamicConfiguration<ConfigType>) staticTenants;
        default:
            return SgDynamicConfiguration.empty();
        }

    }
    
    private void checkForOverriddenEntries(SgDynamicConfiguration<?> original, SgDynamicConfiguration<?> staticConfig) {
        HashSet<String> overridenKeys = new HashSet<>(staticConfig.getCEntries().keySet());
        overridenKeys.retainAll(original.getCEntries().keySet());
        
        if (!overridenKeys.isEmpty()) {
            log.warn("The " + original.getCType().toLCString() + " config tries to override static configuration. This is not possible. Affected config keys: " + overridenKeys);
        }
    }

    private <ConfigType> SgDynamicConfiguration<ConfigType> readConfig(String resourcePath, Class<ConfigType> configType) {
        try {
            JsonNode jsonNode = DefaultObjectMapper.YAML_MAPPER.readTree(DynamicConfigFactory.class.getResourceAsStream(resourcePath));

            return SgDynamicConfiguration.fromNode(jsonNode, configType, 2, 0, 0, 0);
        } catch (Exception e) {
            throw new RuntimeException("Error while reading static configuration from " + resourcePath, e);
        }
    }

}
