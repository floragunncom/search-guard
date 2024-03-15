package com.floragunn.searchguard.configuration.validation;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

@RunWith(MockitoJUnitRunner.class)
public class RoleRelationsValidatorTest {

    @Mock
    private ConfigurationRepository configurationRepository;

    private RoleRelationsValidator roleRelationsValidator;

    @Before
    public void setUp() throws Exception {
        roleRelationsValidator = new RoleRelationsValidator(configurationRepository);
    }

    @Test
    public void configEntry_shouldValidateRelations_noTenantMatchesGivenPattern() throws Exception {
        //there is no tenant
        roleRelationsValidator.setConfigMap(null);
        Role role = Role.parse(DocNode.of("tenant_permissions", DocNode.array(DocNode.of("tenant_patterns", "fake"))), null).get();
        List<ValidationError> validationErrors = roleRelationsValidator.validateConfigEntry(role);
        assertThat(validationErrors, hasSize(1));
        assertThat(validationErrors.get(0).getAttribute(), equalTo("_"));
        assertThat(validationErrors.get(0).getMessage(), equalTo("Tenant pattern: 'fake' does not match any tenant"));

        //there is one tenant
        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(CType.TENANTS,
                        "fake1", Tenant.parse(DocNode.of("description", "test tenant"), null).get())
        );
        roleRelationsValidator.setConfigMap(configMap);
        role = Role.parse(DocNode.of("tenant_permissions", DocNode.array(DocNode.of("tenant_patterns", "fake"))), null).get();
        validationErrors = roleRelationsValidator.validateConfigEntry(role);
        assertThat(validationErrors, hasSize(1));
        assertThat(validationErrors.get(0).getAttribute(), equalTo("_"));
        assertThat(validationErrors.get(0).getMessage(), equalTo("Tenant pattern: 'fake' does not match any tenant"));
    }

    @Test
    public void configEntry_shouldValidateRelations_tenantMatchingGivenPatternExists() throws Exception {
        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(CType.TENANTS,
                        "exists", Tenant.parse(DocNode.of("description", "test tenant"), null).get())
        );
        roleRelationsValidator.setConfigMap(configMap);
        Role role = Role.parse(DocNode.of("tenant_permissions", DocNode.array(DocNode.of("tenant_patterns", "exist*"))), null).get();
        List<ValidationError> validationErrors = roleRelationsValidator.validateConfigEntry(role);
        assertThat(validationErrors, empty());
    }

    @Test
    public void configEntry_shouldDoNothing_configTypeNotSupported() throws Exception {
        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(CType.TENANTS,
                        "exists", Tenant.parse(DocNode.of("description", "test tenant"), null).get())
        );
        roleRelationsValidator.setConfigMap(configMap);
        Tenant tenant = new Tenant(null, false, false, false, null);
        List<ValidationError> validationErrors = roleRelationsValidator.validateConfigEntry(tenant);
        assertThat(validationErrors, empty());
    }

    @Test
    public void configEntry_shouldDoNothing_configIsNull() throws Exception {
        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(CType.TENANTS,
                        "exists", Tenant.parse(DocNode.of("description", "test tenant"), null).get())
        );
        roleRelationsValidator.setConfigMap(configMap);
        Tenant tenant = new Tenant(null, false, false, false, null);
        List<ValidationError> validationErrors = roleRelationsValidator.validateConfigEntry(tenant);
        assertThat(validationErrors, empty());
    }

    @Test
    public void config_shouldValidateRelations_noTenantMatchesOneOfGivenPatterns() throws Exception {
        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(CType.TENANTS,
                        "exists", Tenant.parse(DocNode.of("description", "test tenant"), null).get())
        );
        roleRelationsValidator.setConfigMap(configMap);
        Role roleOne = Role.parse(DocNode.of("tenant_permissions", DocNode.array(DocNode.of("tenant_patterns", "fake"))), null).get();
        Role roleTwo = Role.parse(DocNode.of("tenant_permissions", DocNode.array(DocNode.of("tenant_patterns", "exists"))), null).get();
        SgDynamicConfiguration<Role> roleConfig = SgDynamicConfiguration.of(CType.ROLES, "role1", roleOne, "role2", roleTwo);
        List<ValidationError> validationErrors = roleRelationsValidator.validateConfig(roleConfig);
        assertThat(validationErrors, hasSize(1));
        assertThat(validationErrors.get(0).getAttribute(), equalTo("roles.role1"));
        assertThat(validationErrors.get(0).getMessage(), equalTo("Tenant pattern: 'fake' does not match any tenant"));
    }

    @Test
    public void config_shouldValidateRelations_tenantsMatchingAllOfGivenPatternsExist() throws Exception {
        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(CType.TENANTS,
                        "first", Tenant.parse(DocNode.of("description", "first test tenant"), null).get(),
                        "second", Tenant.parse(DocNode.of("description", "second test tenant"), null).get())
        );
        roleRelationsValidator.setConfigMap(configMap);
        Role roleOne = Role.parse(DocNode.of("tenant_permissions", DocNode.array(DocNode.of("tenant_patterns", "firs*"))), null).get();
        Role roleTwo = Role.parse(DocNode.of("tenant_permissions", DocNode.array(DocNode.of("tenant_patterns", "se*nd"))), null).get();
        SgDynamicConfiguration<Role> roleConfig = SgDynamicConfiguration.of(CType.ROLES, "role1", roleOne, "role2", roleTwo);
        List<ValidationError> validationErrors = roleRelationsValidator.validateConfig(roleConfig);
        assertThat(validationErrors, hasSize(0));
    }

    @Test
    public void config_shouldDoNothing_configTypeNotSupported() throws Exception {
        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(CType.TENANTS,
                        "first", Tenant.parse(DocNode.of("description", "first test tenant"), null).get(),
                        "second", Tenant.parse(DocNode.of("description", "second test tenant"), null).get())
        );
        roleRelationsValidator.setConfigMap(configMap);
        Tenant tenant = new Tenant(null, false, false, false, null);
        SgDynamicConfiguration<Tenant> config = SgDynamicConfiguration.of(CType.TENANTS, "a", tenant);
        List<ValidationError> validationErrors = roleRelationsValidator.validateConfig(config);
        assertThat(validationErrors, hasSize(0));
    }

    @Test
    public void config_shouldDoNothing_configIsNull() throws Exception {
        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(CType.TENANTS,
                        "first", Tenant.parse(DocNode.of("description", "first test tenant"), null).get(),
                        "second", Tenant.parse(DocNode.of("description", "second test tenant"), null).get())
        );
        roleRelationsValidator.setConfigMap(configMap);
        List<ValidationError> validationErrors = roleRelationsValidator.validateConfig(null);
        assertThat(validationErrors, hasSize(0));
    }

    @Test
    public void configList_shouldValidateRelations_newRolesAndTenantsConfigsArePresent_noTenantMatchesOneOfGivenPatterns() throws Exception {
        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(CType.TENANTS,
                        "first", Tenant.parse(DocNode.of("description", "first test tenant"), null).get(),
                        "second", Tenant.parse(DocNode.of("description", "second test tenant"), null).get())
        );
        roleRelationsValidator.setConfigMap(configMap);

        Role roleOne = Role.parse(DocNode.of("tenant_permissions", DocNode.array(DocNode.of("tenant_patterns", "firs*"))), null).get();
        Role roleTwo = Role.parse(DocNode.of("tenant_permissions", DocNode.array(DocNode.of("tenant_patterns", "se*nd"))), null).get();
        SgDynamicConfiguration<Role> newRoleConfig = SgDynamicConfiguration.of(CType.ROLES, "role1", roleOne, "role2", roleTwo);
        SgDynamicConfiguration<Tenant> newTenantConfig = SgDynamicConfiguration.of(CType.TENANTS,
                "second", Tenant.parse(DocNode.of("description", "second test tenant"), null).get(),
                "third", Tenant.parse(DocNode.of("description", "third test tenant"), null).get()
        );

        List<ValidationError> validationErrors = roleRelationsValidator.validateConfigs(ImmutableList.of(newRoleConfig, newTenantConfig));
        assertThat(validationErrors, hasSize(1));
        assertThat(validationErrors.get(0).getAttribute(), equalTo("roles.role1"));
        assertThat(validationErrors.get(0).getMessage(), equalTo("Tenant pattern: 'firs*' does not match any tenant"));
    }

    @Test
    public void configList_shouldValidateRelations_newRolesAndTenantsConfigsArePresent_tenantsMatchingAllOfGivenPatternsExist() throws Exception {
        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(CType.TENANTS,
                        "first", Tenant.parse(DocNode.of("description", "first test tenant"), null).get(),
                        "second", Tenant.parse(DocNode.of("description", "second test tenant"), null).get())
        );
        roleRelationsValidator.setConfigMap(configMap);

        Role roleOne = Role.parse(DocNode.of("tenant_permissions", DocNode.array(DocNode.of("tenant_patterns", "thi*"))), null).get();
        Role roleTwo = Role.parse(DocNode.of("tenant_permissions", DocNode.array(DocNode.of("tenant_patterns", "fo*th"))), null).get();
        SgDynamicConfiguration<Role> newRoleConfig = SgDynamicConfiguration.of(CType.ROLES, "role1", roleOne, "role2", roleTwo);
        SgDynamicConfiguration<Tenant> newTenantConfig = SgDynamicConfiguration.of(CType.TENANTS,
                "third", Tenant.parse(DocNode.of("description", "third test tenant"), null).get(),
                "fourth", Tenant.parse(DocNode.of("description", "fourth test tenant"), null).get()
        );

        List<ValidationError> validationErrors = roleRelationsValidator.validateConfigs(ImmutableList.of(newRoleConfig, newTenantConfig));
        assertThat(validationErrors, hasSize(0));
    }

    @Test
    public void configList_shouldValidateRelations_onlyNewRolesConfigIsPresent_noTenantMatchesOneOfGivenPatterns() throws Exception {
        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(CType.TENANTS,
                        "first", Tenant.parse(DocNode.of("description", "first test tenant"), null).get(),
                        "second", Tenant.parse(DocNode.of("description", "second test tenant"), null).get())
        );
        roleRelationsValidator.setConfigMap(configMap);

        Role roleOne = Role.parse(DocNode.of("tenant_permissions", DocNode.array(DocNode.of("tenant_patterns", "firs*"))), null).get();
        Role roleTwo = Role.parse(DocNode.of("tenant_permissions", DocNode.array(DocNode.of("tenant_patterns", "se*nds"))), null).get();
        SgDynamicConfiguration<Role> newRoleConfig = SgDynamicConfiguration.of(CType.ROLES, "role1", roleOne, "role2", roleTwo);

        List<ValidationError> validationErrors = roleRelationsValidator.validateConfigs(ImmutableList.of(newRoleConfig));
        assertThat(validationErrors, hasSize(1));
        assertThat(validationErrors.get(0).getAttribute(), equalTo("roles.role2"));
        assertThat(validationErrors.get(0).getMessage(), equalTo("Tenant pattern: 'se*nds' does not match any tenant"));
    }

    @Test
    public void configList_shouldValidateRelations_onlyNewRolesConfigIsPresent_tenantsMatchingAllOfGivenPatternsExist() throws Exception {
        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(CType.TENANTS,
                        "first", Tenant.parse(DocNode.of("description", "first test tenant"), null).get(),
                        "second", Tenant.parse(DocNode.of("description", "second test tenant"), null).get())
        );
        roleRelationsValidator.setConfigMap(configMap);

        Role roleOne = Role.parse(DocNode.of("tenant_permissions", DocNode.array(DocNode.of("tenant_patterns", "f*"))), null).get();
        Role roleTwo = Role.parse(DocNode.of("tenant_permissions", DocNode.array(DocNode.of("tenant_patterns", "s*"))), null).get();
        SgDynamicConfiguration<Role> newRoleConfig = SgDynamicConfiguration.of(CType.ROLES, "role1", roleOne, "role2", roleTwo);

        List<ValidationError> validationErrors = roleRelationsValidator.validateConfigs(ImmutableList.of(newRoleConfig));
        assertThat(validationErrors, hasSize(0));
    }

    @Test
    public void configList_shouldValidateRelations_shouldDoNothing_listIsNull() throws Exception {
        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(CType.TENANTS,
                        "first", Tenant.parse(DocNode.of("description", "first test tenant"), null).get(),
                        "second", Tenant.parse(DocNode.of("description", "second test tenant"), null).get())
        );
        roleRelationsValidator.setConfigMap(configMap);

        List<ValidationError> validationErrors = roleRelationsValidator.validateConfigs(null);
        assertThat(validationErrors, hasSize(0));
    }

    @Test
    public void configList_shouldValidateRelations_shouldDoNothing_listContainsOnlyNullElement() throws Exception {
        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(CType.TENANTS,
                        "first", Tenant.parse(DocNode.of("description", "first test tenant"), null).get(),
                        "second", Tenant.parse(DocNode.of("description", "second test tenant"), null).get())
        );
        roleRelationsValidator.setConfigMap(configMap);

        List<ValidationError> validationErrors = roleRelationsValidator.validateConfigs(ImmutableList.of(null));
        assertThat(validationErrors, hasSize(0));
    }

    private ConfigMap configMapWithConfig(SgDynamicConfiguration<?> config) {
        return new ConfigMap.Builder("index").with(config).build();
    }
}
