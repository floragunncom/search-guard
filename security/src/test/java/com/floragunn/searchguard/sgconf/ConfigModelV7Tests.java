package com.floragunn.searchguard.sgconf;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.modules.SearchGuardModulesRegistry;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.sgconf.impl.v7.ActionGroupsV7;
import com.floragunn.searchguard.sgconf.impl.v7.BlocksV7;
import com.floragunn.searchguard.sgconf.impl.v7.ConfigV7;
import com.floragunn.searchguard.sgconf.impl.v7.RoleMappingsV7;
import com.floragunn.searchguard.sgconf.impl.v7.RoleV7;
import com.floragunn.searchguard.sgconf.impl.v7.TenantV7;
import com.floragunn.searchguard.user.User;
import com.google.common.collect.ImmutableMap;

public class ConfigModelV7Tests {

    private static SearchGuardModulesRegistry searchGuardModulesRegistry = new SearchGuardModulesRegistry(Settings.EMPTY);

    @Test
    public void testWildcardTenantMapping() throws IOException, ConfigValidationException {
        ConfigV7 configV7 = new ConfigV7();
        configV7.dynamic = new ConfigV7.Dynamic();

        DynamicConfigModelV7 dynamicConfigModel = new DynamicConfigModelV7(configV7, null, Settings.EMPTY, null, searchGuardModulesRegistry, Collections.emptyList(), null);
        SgDynamicConfiguration<RoleV7> roles = SgDynamicConfiguration
                .fromMap(
                        ImmutableMap.of("_sg_meta", ImmutableMap.of("type", "roles", "config_version", 2), "all_access",
                                ImmutableMap.of("tenant_permissions",
                                        Arrays.asList(
                                                ImmutableMap.of("tenant_patterns", Arrays.asList("*"), "allowed_actions", Arrays.asList("*"))))),
                        CType.ROLES, 2, -1, -1, -1, null);
        SgDynamicConfiguration<RoleMappingsV7> rolemappings = SgDynamicConfiguration.empty();
        SgDynamicConfiguration<ActionGroupsV7> actiongroups = SgDynamicConfiguration.empty();
        SgDynamicConfiguration<TenantV7> tenants = SgDynamicConfiguration.fromMap(ImmutableMap.of("_sg_meta",
                ImmutableMap.of("type", "tenants", "config_version", 2), "my_tenant", ImmutableMap.of("description", "my tenant")), CType.TENANTS, 2,
                -1, -1, -1, null);
        SgDynamicConfiguration<BlocksV7> blocks = SgDynamicConfiguration.empty();

        ConfigModel configModel = new ConfigModelV7(roles, rolemappings, actiongroups, tenants, blocks, dynamicConfigModel, Settings.EMPTY);

        User user = User.forUser("test").searchGuardRoles("all_access").build();
        SgRoles sgRoles = configModel.getSgRoles();
        SgRoles filteredSgRoles = sgRoles.filter(configModel.mapSgRoles(user, null));
        Assert.assertEquals(ImmutableMap.of("test", true, "my_tenant", true),
                filteredSgRoles.mapTenants(user, configModel.getAllConfiguredTenantNames()));
    }
}
