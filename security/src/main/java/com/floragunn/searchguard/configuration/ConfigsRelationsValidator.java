/*
 * Copyright 2023 floragunn GmbH
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

package com.floragunn.searchguard.configuration;

import com.floragunn.codova.config.templates.Template;
import com.floragunn.codova.config.text.Pattern;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.authz.config.Tenant;
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

public class ConfigsRelationsValidator {

    private static final Logger log = LogManager.getLogger(ConfigsRelationsValidator.class);
    private ConfigMap configMap;

    ConfigsRelationsValidator(ConfigurationRepository configurationRepository) {
        Objects.requireNonNull(configurationRepository, "Configuration repository is required");
        configurationRepository.subscribeOnChange(this::onCofigurationChange);
    }

    public List<ValidationError> validateConfigsRelations(List<SgDynamicConfiguration<?>> newConfigs) {
        List<SgDynamicConfiguration<?>> notNullConfigs = newConfigs.stream().filter(Objects::nonNull).collect(Collectors.toList());

        List<ValidationError> errors = new ArrayList<>();

        Optional<SgDynamicConfiguration<Role>> newRolesConfig = findConfigOfType(CType.ROLES.getType(), notNullConfigs);
        errors.addAll(newRolesConfig.map(roles -> validateRolesRelations(roles, notNullConfigs)).orElse(Collections.emptyList()));

        return errors;
    }

    public List<ValidationError> validateConfigRelations(SgDynamicConfiguration<?> config) {
        return validateConfigsRelations(Collections.singletonList(config));
    }

    public <T> List<ValidationError> validateConfigEntryRelations(T entry) {
        if (Objects.nonNull(entry)) {
            List<ValidationError> errors = new ArrayList<>();

            if (CType.ROLES.getType().isAssignableFrom(entry.getClass())) {
                SgDynamicConfiguration<Tenant> tenantsConfig = getConfigFromMap(CType.TENANTS);
                errors.addAll(validateRoleEntryRelations(null, (Role) entry, tenantsConfig));
            }

            return errors;
        }
        return Collections.emptyList();
    }

    void onCofigurationChange(ConfigMap configMap) {
        this.configMap = configMap;
    }

    private List<ValidationError> validateRolesRelations(SgDynamicConfiguration<Role> newRolesConfig, List<SgDynamicConfiguration<?>> newConfigs) {
        List<ValidationError> errors = new ArrayList<>();

        SgDynamicConfiguration<Tenant> existingTenantsConfig = getConfigFromMap(CType.TENANTS);

        SgDynamicConfiguration<Tenant> newTenantsConfig = findConfigOfType(CType.TENANTS.getType(), newConfigs)
                .map(newTenants -> newTenants.withStatic(getStaticConfigEntries(existingTenantsConfig)))
                .orElse(existingTenantsConfig);

        newRolesConfig.getCEntries().forEach((roleName, role) -> {
            String attribute = String.format("%s.%s", CType.ROLES.getName(), roleName);
            errors.addAll(validateRoleEntryRelations(attribute, role, newTenantsConfig));
        });

        return errors;
    }

    private List<ValidationError> validateRoleEntryRelations(String attribute, Role roleConfig, SgDynamicConfiguration<Tenant> tenantsConfig) {
        List<ValidationError> errors = new ArrayList<>();

        ImmutableSet<String> tenantNames = tenantsConfig.getCEntries().keySet().with(Tenant.GLOBAL_TENANT_ID);

        roleConfig.getTenantPermissions().forEach(tenant -> tenant.getTenantPatterns().forEach(tenantPattern -> {
            if (! tenantPatternMatchesAnyTenant(tenantPattern, tenantNames)) {
                String msg = String.format("Tenant pattern: '%s' does not match any tenant", tenantPattern.getSource());
                errors.add(new ValidationError(attribute, msg));
            }
        }));

        return errors;
    }

    @SuppressWarnings("unchecked")
    private <T> Optional<SgDynamicConfiguration<T>> findConfigOfType(Class<T> type, List<SgDynamicConfiguration<?>> newConfigs) {
        return newConfigs.stream().filter(config -> config.getCType().getType().isAssignableFrom(type))
                .findFirst()
                .map(config -> (SgDynamicConfiguration<T>) config);
    }

    private <T> SgDynamicConfiguration<T> getConfigFromMap(CType<T> typeToLoad) {
        return Optional.ofNullable(configMap)
                .map(confMap -> confMap.get(typeToLoad))
                .orElseGet(() -> {
                    log.warn("Config of type {} is unavailable, an empty config will be used instead", typeToLoad.getName());
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
