/*
 * Copyright 2024 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.floragunn.searchguard.configuration.validation;

import com.floragunn.codova.config.templates.Template;
import com.floragunn.codova.config.text.Pattern;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class RoleRelationsValidator extends ConfigModificationValidator<Role> {

    private static final Logger log = LogManager.getLogger(RoleRelationsValidator.class);
    private ConfigMap configMap;

    public RoleRelationsValidator(ConfigurationRepository configurationRepository) {
        super(CType.ROLES, configurationRepository);
    }

    @Override
    public List<ValidationError> validateConfigs(List<SgDynamicConfiguration<?>> newConfigs) {
        List<SgDynamicConfiguration<?>> notNullConfigs = Optional.ofNullable(newConfigs).orElse(new ArrayList<>())
                .stream().filter(Objects::nonNull).collect(Collectors.toList());

        List<ValidationError> errors = new ArrayList<>();

        Optional<SgDynamicConfiguration<Role>> newRolesConfig = findConfigOfType(Role.class, notNullConfigs);
        errors.addAll(newRolesConfig.map(roles -> validateRolesRelations(roles, notNullConfigs)).orElse(Collections.emptyList()));

        return errors;
    }

    @Override
    public List<ValidationError> validateConfig(SgDynamicConfiguration<?> newConfig) {
        return validateConfigs(Collections.singletonList(newConfig));
    }

    @Override
    public <T> List<ValidationError> validateConfigEntry(T newConfigEntry) {
        if (Objects.nonNull(newConfigEntry)) {
            List<ValidationError> errors = new ArrayList<>();

            if (Role.class.isAssignableFrom(newConfigEntry.getClass())) {
                SgDynamicConfiguration<Tenant> tenantsConfig = getConfigFromMap(CType.TENANTS);
                errors.addAll(validateRoleEntryRelations(null, (Role) newConfigEntry, tenantsConfig));
            }

            return errors;
        }
        return Collections.emptyList();
    }

    private List<ValidationError> validateRolesRelations(SgDynamicConfiguration<Role> newRolesConfig, List<SgDynamicConfiguration<?>> newConfigs) {
        List<ValidationError> errors = new ArrayList<>();

        SgDynamicConfiguration<Tenant> existingTenantsConfig = getConfigFromMap(CType.TENANTS);

        SgDynamicConfiguration<Tenant> newTenantsConfig = findConfigOfType(CType.TENANTS.getType(), newConfigs)
                .map(newTenants -> newTenants.with(getStaticConfigEntries(existingTenantsConfig)))
                .orElse(existingTenantsConfig);

        newRolesConfig.getCEntries().forEach((roleName, role) -> {
            errors.addAll(validateRoleEntryRelations(roleName, role, newTenantsConfig));
        });

        return errors;
    }

    private List<ValidationError> validateRoleEntryRelations(String configEntryKey, Role roleConfig, SgDynamicConfiguration<Tenant> tenantsConfig) {
        List<ValidationError> errors = new ArrayList<>();

        ImmutableSet<String> tenantNames = tenantsConfig.getCEntries().keySet().with(Tenant.GLOBAL_TENANT_ID);

        roleConfig.getTenantPermissions().forEach(tenant -> tenant.getTenantPatterns().forEach(tenantPattern -> {
            if (! tenantPatternMatchesAnyTenant(tenantPattern, tenantNames)) {
                String msg = String.format("Tenant pattern: '%s' does not match any tenant", tenantPattern.getSource());
                errors.add(toValidationError(configEntryKey, msg));
            }
        }));

        return errors;
    }

    private <T> SgDynamicConfiguration<T> getConfigFromMap(CType<T> typeToLoad) {
        return findCurrentConfiguration(typeToLoad)
                .orElseGet(() -> {
                    log.debug("Config of type {} is unavailable, an empty config will be used instead", typeToLoad.getName());
                    return SgDynamicConfiguration.empty(typeToLoad);
                });
    }

    private boolean tenantPatternMatchesAnyTenant(Template<Pattern> tenantPattern, ImmutableSet<String> tenantNames) {
        return ! tenantNames.isEmpty() && ! tenantPattern.getConstantValue().getMatching(tenantNames).isEmpty();
    }

    private <T> Map<String, T> getStaticConfigEntries(SgDynamicConfiguration<T> config) {
        Set<String> nonStaticConfigNames = config.withoutStatic().getCEntries().keySet();
        return config.getCEntries()
                .entrySet()
                .stream()
                .filter(configEntry -> ! nonStaticConfigNames.contains(configEntry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

}
