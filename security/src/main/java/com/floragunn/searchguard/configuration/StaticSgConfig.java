package com.floragunn.searchguard.configuration;

import java.util.HashSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.searchguard.authz.config.ActionGroup;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.support.ConfigConstants;

public class StaticSgConfig {
    private static final Logger log = LogManager.getLogger(StaticSgConfig.class);

    private final SgDynamicConfiguration<Role> staticRoles;
    private final SgDynamicConfiguration<ActionGroup> staticActionGroups;
    private final SgDynamicConfiguration<Tenant> staticTenants;

    public StaticSgConfig(Settings settings) {
        if (settings.getAsBoolean(ConfigConstants.SEARCHGUARD_UNSUPPORTED_LOAD_STATIC_RESOURCES, true)) {
            staticRoles = readConfig("/static_config/static_roles.yml", CType.ROLES);
            staticActionGroups = readConfig("/static_config/static_action_groups.yml", CType.ACTIONGROUPS);
            staticTenants = readConfig("/static_config/static_tenants.yml", CType.TENANTS);
        } else {
            log.info("searchguard.unsupported.load_static_resources is set to false. Static resources will not be loaded.");
            staticRoles = SgDynamicConfiguration.empty(CType.ROLES);
            staticActionGroups = SgDynamicConfiguration.empty(CType.ACTIONGROUPS);
            staticTenants = SgDynamicConfiguration.empty(CType.TENANTS);
        }
    }

    public <T> SgDynamicConfiguration<T> addTo(SgDynamicConfiguration<T> original) {
        SgDynamicConfiguration<T> staticConfig = get(original);
        
        if (staticConfig.getCEntries().isEmpty()) {
            return original;
        }
        
        checkForOverriddenEntries(original, staticConfig);

        SgDynamicConfiguration<T> result = original.with(staticConfig.getCEntries());

        if (log.isDebugEnabled()) {
            log.debug(staticConfig.getCEntries().size() + " static " + original.getCType().toLCString() + " loaded");
        }
        
        return result;
    }

    @SuppressWarnings("unchecked")
    public <ConfigType> SgDynamicConfiguration<ConfigType> get(SgDynamicConfiguration<ConfigType> original) {
        if (original.getCType().equals(CType.ACTIONGROUPS)) {
            return (SgDynamicConfiguration<ConfigType>) staticActionGroups;
        } else if (original.getCType().equals(CType.ROLES)) {
            return (SgDynamicConfiguration<ConfigType>) staticRoles;
        } else if (original.getCType().equals(CType.TENANTS)) {
            return (SgDynamicConfiguration<ConfigType>) staticTenants;
        } else {
            return SgDynamicConfiguration.empty(original.getCType());
        }
    }
    
    private void checkForOverriddenEntries(SgDynamicConfiguration<?> original, SgDynamicConfiguration<?> staticConfig) {
        HashSet<String> overridenKeys = new HashSet<>(staticConfig.getCEntries().keySet());
        overridenKeys.retainAll(original.getCEntries().keySet());
        
        if (!overridenKeys.isEmpty()) {
            log.warn("The " + original.getCType().toLCString()
                    + " config tries to override static configuration. This is not possible. Affected config keys: " + overridenKeys + "; type: "
                    + original.getCType() + "; v: " + original.getDocVersion(), new Exception());
        }
    }

    private <ConfigType> SgDynamicConfiguration<ConfigType> readConfig(String resourcePath, CType<ConfigType> configType) {
        try {
            DocNode docNode = DocNode.parse(Format.YAML).from(StaticSgConfig.class.getResourceAsStream(resourcePath));

            return SgDynamicConfiguration.fromDocNode(docNode, docNode.toJsonString(), configType, 0l, 0l, 0l, null).get();
        } catch (Exception e) {
            throw new RuntimeException("Error while reading static configuration from " + resourcePath, e);
        }
    }

}
