/*
 * Copyright 2023-2024 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */
package com.floragunn.searchguard.enterprise.femt;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchsupport.util.EsLogging;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FeMultiTenancyEnabledFlagValidatorTest {

    static {
        EsLogging.initLogging();
    }

    @Mock
    private ConfigurationRepository configurationRepository;

    @Mock
    private FeMultiTenancyConfigurationProvider feMultiTenancyConfigurationProvider;

    @Mock
    private ClusterService clusterService;
    @Mock
    private ClusterState clusterState;
    @Mock
    private Metadata metadata;
    @Mock
    private ProjectMetadata projectMetadata;

    private static final String KIBANA_INDEX = ".kibana";

    private FeMultiTenancyEnabledFlagValidator feMultiTenancyEnabledFlagValidator;

    @Before
    public void setUp() throws Exception {
        feMultiTenancyEnabledFlagValidator = new FeMultiTenancyEnabledFlagValidator(
                feMultiTenancyConfigurationProvider, clusterService, configurationRepository
        );
        when(feMultiTenancyConfigurationProvider.getKibanaIndex()).thenReturn(KIBANA_INDEX);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
    }

    @Test
    public void configEntry_shouldValidateFeMtEnabledFlag_valueChanges_thereIsKibanaIndex() throws Exception {
        when(metadata.getProject(Metadata.DEFAULT_PROJECT_ID)).thenReturn(projectMetadata);
        when(projectMetadata.getIndicesLookup()).thenReturn(new TreeMap<>(ImmutableMap.of(KIBANA_INDEX + "_1.2.3", null)));

        //try to disable MT
        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE,
                        "default", FeMultiTenancyConfig.parse(DocNode.of("enabled", true), null).get())
        );
        feMultiTenancyEnabledFlagValidator.setConfigMap(configMap);

        FeMultiTenancyConfig feMtConfig = FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get();
        List<ValidationError> validationErrors = feMultiTenancyEnabledFlagValidator.validateConfigEntry(feMtConfig);
        assertThat(validationErrors, hasSize(1));
        assertThat(validationErrors.get(0).getAttribute(), equalTo("_"));
        assertThat(validationErrors.get(0).getMessage(), equalTo("Cannot change the value of the 'enabled' flag to 'false'. Multitenancy cannot be disabled, please contact the support team."));

        //try to enable MT
        configMap = configMapWithConfig(
                SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE,
                        "default", FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get())
        );
        feMultiTenancyEnabledFlagValidator.setConfigMap(configMap);

        feMtConfig = FeMultiTenancyConfig.parse(DocNode.of("enabled", true), null).get();
        validationErrors = feMultiTenancyEnabledFlagValidator.validateConfigEntry(feMtConfig);
        assertThat(validationErrors, hasSize(1));
        assertThat(validationErrors.get(0).getAttribute(), equalTo("_"));
        assertThat(validationErrors.get(0).getMessage(), equalTo("You try to enable multitenancy. This operation cannot be undone. Please use the 'sgctl.sh special enable-mt' command if you are sure that you want to proceed."));


        //should compare to default (disabled) configuration
        feMultiTenancyEnabledFlagValidator.setConfigMap(null);

        feMtConfig = FeMultiTenancyConfig.parse(DocNode.of("enabled", true), null).get();
        validationErrors = feMultiTenancyEnabledFlagValidator.validateConfigEntry(feMtConfig);
        assertThat(validationErrors, hasSize(1));
        assertThat(validationErrors.get(0).getAttribute(), equalTo("_"));
        assertThat(validationErrors.get(0).getMessage(), equalTo("You try to enable multitenancy. This operation cannot be undone. Please use the 'sgctl.sh special enable-mt' command if you are sure that you want to proceed."));
    }

    @Test
    public void configEntry_shouldValidateFeMtEnabledFlag_valueChanges_thereIsNoKibanaIndex() throws Exception {
        when(metadata.getProject(Metadata.DEFAULT_PROJECT_ID)).thenReturn(projectMetadata);
        when(projectMetadata.getIndicesLookup()).thenReturn(new TreeMap<>());

        //try to disable MT
        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE,
                        "default", FeMultiTenancyConfig.parse(DocNode.of("enabled", true), null).get())
        );
        feMultiTenancyEnabledFlagValidator.setConfigMap(configMap);

        FeMultiTenancyConfig feMtConfig = FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get();
        List<ValidationError> validationErrors = feMultiTenancyEnabledFlagValidator.validateConfigEntry(feMtConfig);
        assertThat(validationErrors, hasSize(0));

        //try to enable MT
        configMap = configMapWithConfig(
                SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE,
                        "default", FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get())
        );
        feMultiTenancyEnabledFlagValidator.setConfigMap(configMap);

        feMtConfig = FeMultiTenancyConfig.parse(DocNode.of("enabled", true), null).get();
        validationErrors = feMultiTenancyEnabledFlagValidator.validateConfigEntry(feMtConfig);
        assertThat(validationErrors, hasSize(0));


        //should compare to default (disabled) configuration
        feMultiTenancyEnabledFlagValidator.setConfigMap(null);

        feMtConfig = FeMultiTenancyConfig.parse(DocNode.of("enabled", true), null).get();
        validationErrors = feMultiTenancyEnabledFlagValidator.validateConfigEntry(feMtConfig);
        assertThat(validationErrors, hasSize(0));
    }

    @Test
    public void configEntry_shouldValidateFeMtEnabledFlag_valueDoesNotChange() throws Exception {

        //MT disabled
        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE,
                        "default", FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get())
        );
        feMultiTenancyEnabledFlagValidator.setConfigMap(configMap);

        FeMultiTenancyConfig feMtConfig = FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get();
        List<ValidationError> validationErrors = feMultiTenancyEnabledFlagValidator.validateConfigEntry(feMtConfig);
        assertThat(validationErrors, hasSize(0));

        //should compare to default (disabled) configuration
        feMultiTenancyEnabledFlagValidator.setConfigMap(null);

        feMtConfig = FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get();
        validationErrors = feMultiTenancyEnabledFlagValidator.validateConfigEntry(feMtConfig);
        assertThat(validationErrors, hasSize(0));

        //MT enabled
        configMap = configMapWithConfig(
                SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE,
                        "default", FeMultiTenancyConfig.parse(DocNode.of("enabled", true), null).get())
        );
        feMultiTenancyEnabledFlagValidator.setConfigMap(configMap);

        feMtConfig = FeMultiTenancyConfig.parse(DocNode.of("enabled", true), null).get();
        validationErrors = feMultiTenancyEnabledFlagValidator.validateConfigEntry(feMtConfig);
        assertThat(validationErrors, hasSize(0));

        verifyNoInteractions(metadata);
    }

    @Test
    public void configEntry_shouldDoNothing_configTypeNotSupported() throws Exception {

        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE,
                        "default", FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get())
        );
        feMultiTenancyEnabledFlagValidator.setConfigMap(configMap);
        Role role = Role.parse(DocNode.of("cluster_permissions", "*"), null).get();
        List<ValidationError> validationErrors = feMultiTenancyEnabledFlagValidator.validateConfigEntry(role);
        assertThat(validationErrors, hasSize(0));

        verifyNoInteractions(metadata);
    }

    @Test
    public void configEntry_shouldDoNothing_configIsNull() throws Exception {

        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE,
                        "default", FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get())
        );
        feMultiTenancyEnabledFlagValidator.setConfigMap(configMap);
        List<ValidationError> validationErrors = feMultiTenancyEnabledFlagValidator.validateConfigEntry(null);
        assertThat(validationErrors, hasSize(0));

        verifyNoInteractions(metadata);
    }

    @Test
    public void config_shouldValidateFeMtEnabledFlag_valueChanges_thereIsKibanaIndex() throws Exception {
        when(metadata.getProject(Metadata.DEFAULT_PROJECT_ID)).thenReturn(projectMetadata);
        when(projectMetadata.getIndicesLookup()).thenReturn(new TreeMap<>(ImmutableMap.of(KIBANA_INDEX, null)));

        //try to disable MT
        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE,
                        "default", FeMultiTenancyConfig.parse(DocNode.of("enabled", true), null).get())
        );
        feMultiTenancyEnabledFlagValidator.setConfigMap(configMap);

        FeMultiTenancyConfig feMtConfig = FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get();
        SgDynamicConfiguration<FeMultiTenancyConfig> newConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", feMtConfig);
        List<ValidationError> validationErrors = feMultiTenancyEnabledFlagValidator.validateConfig(newConfig);
        assertThat(validationErrors, hasSize(1));
        assertThat(validationErrors.get(0).getAttribute(), equalTo("frontend_multi_tenancy.default"));
        assertThat(validationErrors.get(0).getMessage(), equalTo("Cannot change the value of the 'enabled' flag to 'false'. Multitenancy cannot be disabled, please contact the support team."));

        //try to enable MT
        configMap = configMapWithConfig(
                SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE,
                        "default", FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get())
        );
        feMultiTenancyEnabledFlagValidator.setConfigMap(configMap);

        feMtConfig = FeMultiTenancyConfig.parse(DocNode.of("enabled", true), null).get();
        newConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", feMtConfig);
        validationErrors = feMultiTenancyEnabledFlagValidator.validateConfig(newConfig);
        assertThat(validationErrors, hasSize(1));
        assertThat(validationErrors.get(0).getAttribute(), equalTo("frontend_multi_tenancy.default"));
        assertThat(validationErrors.get(0).getMessage(), equalTo("You try to enable multitenancy. This operation cannot be undone. Please use the 'sgctl.sh special enable-mt' command if you are sure that you want to proceed."));


        //should compare to default (disabled) configuration
        feMultiTenancyEnabledFlagValidator.setConfigMap(null);

        feMtConfig = FeMultiTenancyConfig.parse(DocNode.of("enabled", true), null).get();
        newConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", feMtConfig);
        validationErrors = feMultiTenancyEnabledFlagValidator.validateConfig(newConfig);
        assertThat(validationErrors, hasSize(1));
        assertThat(validationErrors.get(0).getAttribute(), equalTo("frontend_multi_tenancy.default"));
        assertThat(validationErrors.get(0).getMessage(), equalTo("You try to enable multitenancy. This operation cannot be undone. Please use the 'sgctl.sh special enable-mt' command if you are sure that you want to proceed."));
    }

    @Test
    public void config_shouldValidateFeMtEnabledFlag_valueChanges_thereIsNoKibanaIndex() throws Exception {

        when(metadata.getProject(Metadata.DEFAULT_PROJECT_ID)).thenReturn(projectMetadata);
        when(projectMetadata.getIndicesLookup()).thenReturn(new TreeMap<>());

        //try to disable MT
        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE,
                        "default", FeMultiTenancyConfig.parse(DocNode.of("enabled", true), null).get())
        );
        feMultiTenancyEnabledFlagValidator.setConfigMap(configMap);

        FeMultiTenancyConfig feMtConfig = FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get();
        SgDynamicConfiguration<FeMultiTenancyConfig> newConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", feMtConfig);
        List<ValidationError> validationErrors = feMultiTenancyEnabledFlagValidator.validateConfig(newConfig);
        assertThat(validationErrors, hasSize(0));

        //try to enable MT
        configMap = configMapWithConfig(
                SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE,
                        "default", FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get())
        );
        feMultiTenancyEnabledFlagValidator.setConfigMap(configMap);

        feMtConfig = FeMultiTenancyConfig.parse(DocNode.of("enabled", true), null).get();
        newConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", feMtConfig);
        validationErrors = feMultiTenancyEnabledFlagValidator.validateConfig(newConfig);
        assertThat(validationErrors, hasSize(0));


        //should compare to default (disabled) configuration
        feMultiTenancyEnabledFlagValidator.setConfigMap(null);

        feMtConfig = FeMultiTenancyConfig.parse(DocNode.of("enabled", true), null).get();
        newConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", feMtConfig);
        validationErrors = feMultiTenancyEnabledFlagValidator.validateConfig(newConfig);
        assertThat(validationErrors, hasSize(0));
    }

    @Test
    public void config_shouldValidateFeMtEnabledFlag_valueDoesNotChange() throws Exception {

        //MT disabled
        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE,
                        "default", FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get())
        );
        feMultiTenancyEnabledFlagValidator.setConfigMap(configMap);

        FeMultiTenancyConfig feMtConfig = FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get();
        SgDynamicConfiguration<FeMultiTenancyConfig> newConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", feMtConfig);
        List<ValidationError> validationErrors = feMultiTenancyEnabledFlagValidator.validateConfig(newConfig);
        assertThat(validationErrors, hasSize(0));

        //should compare to default (disabled) configuration
        feMultiTenancyEnabledFlagValidator.setConfigMap(null);

        feMtConfig = FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get();
        newConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", feMtConfig);
        validationErrors = feMultiTenancyEnabledFlagValidator.validateConfig(newConfig);
        assertThat(validationErrors, hasSize(0));

        //MT enabled
        configMap = configMapWithConfig(
                SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE,
                        "default", FeMultiTenancyConfig.parse(DocNode.of("enabled", true), null).get())
        );
        feMultiTenancyEnabledFlagValidator.setConfigMap(configMap);

        feMtConfig = FeMultiTenancyConfig.parse(DocNode.of("enabled", true), null).get();
        newConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", feMtConfig);
        validationErrors = feMultiTenancyEnabledFlagValidator.validateConfig(newConfig);
        assertThat(validationErrors, hasSize(0));

        verifyNoInteractions(metadata);
    }

    @Test
    public void config_shouldDoNothing_configTypeNotSupported() throws Exception {

        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE,
                        "default", FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get())
        );
        feMultiTenancyEnabledFlagValidator.setConfigMap(configMap);
        Role role = Role.parse(DocNode.of("cluster_permissions", "*"), null).get();
        SgDynamicConfiguration<Role> newConfig = SgDynamicConfiguration.of(CType.ROLES, "name", role);
        List<ValidationError> validationErrors = feMultiTenancyEnabledFlagValidator.validateConfig(newConfig);
        assertThat(validationErrors, hasSize(0));

        verifyNoInteractions(metadata);
    }

    @Test
    public void config_shouldDoNothing_configIsNull() throws Exception {

        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE,
                        "default", FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get())
        );
        feMultiTenancyEnabledFlagValidator.setConfigMap(configMap);
        List<ValidationError> validationErrors = feMultiTenancyEnabledFlagValidator.validateConfig(null);
        assertThat(validationErrors, hasSize(0));

        verifyNoInteractions(metadata);
    }

    @Test
    public void config_shouldDoNothing_configIsEmpty() throws Exception {

        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE,
                        "default", FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get())
        );
        feMultiTenancyEnabledFlagValidator.setConfigMap(configMap);
        SgDynamicConfiguration<FeMultiTenancyConfig> newConfig = SgDynamicConfiguration.empty(FeMultiTenancyConfig.TYPE);
        List<ValidationError> validationErrors = feMultiTenancyEnabledFlagValidator.validateConfig(newConfig);
        assertThat(validationErrors, hasSize(0));

        verifyNoInteractions(metadata);
    }

    @Test
    public void config_shouldDoNothing_configWithUnsupportedKey() throws Exception {

        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE,
                        "default", FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get())
        );
        feMultiTenancyEnabledFlagValidator.setConfigMap(configMap);
        FeMultiTenancyConfig feMtConfig = FeMultiTenancyConfig.parse(DocNode.of("enabled", true), null).get();
        SgDynamicConfiguration<FeMultiTenancyConfig> newConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "unsupported", feMtConfig);
        List<ValidationError> validationErrors = feMultiTenancyEnabledFlagValidator.validateConfig(newConfig);
        assertThat(validationErrors, hasSize(0));

        verifyNoInteractions(metadata);
    }

    @Test
    public void configList_shouldValidateFeMtEnabledFlag_valueChanges_thereIsKibanaIndex() throws Exception {
        when(metadata.getProject(Metadata.DEFAULT_PROJECT_ID)).thenReturn(projectMetadata);
        when(projectMetadata.getIndicesLookup()).thenReturn(new TreeMap<>(ImmutableMap.of(KIBANA_INDEX + "001", null)));

        //try to disable MT
        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE,
                        "default", FeMultiTenancyConfig.parse(DocNode.of("enabled", true), null).get())
        );
        feMultiTenancyEnabledFlagValidator.setConfigMap(configMap);

        FeMultiTenancyConfig feMtConfig = FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get();
        SgDynamicConfiguration<FeMultiTenancyConfig> newConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", feMtConfig);
        List<ValidationError> validationErrors = feMultiTenancyEnabledFlagValidator.validateConfigs(Collections.singletonList(newConfig));
        assertThat(validationErrors, hasSize(1));
        assertThat(validationErrors.get(0).getAttribute(), equalTo("frontend_multi_tenancy.default"));
        assertThat(validationErrors.get(0).getMessage(), equalTo("Cannot change the value of the 'enabled' flag to 'false'. Multitenancy cannot be disabled, please contact the support team."));

        //try to enable MT
        configMap = configMapWithConfig(
                SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE,
                        "default", FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get())
        );
        feMultiTenancyEnabledFlagValidator.setConfigMap(configMap);

        feMtConfig = FeMultiTenancyConfig.parse(DocNode.of("enabled", true), null).get();
        newConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", feMtConfig);
        validationErrors = feMultiTenancyEnabledFlagValidator.validateConfigs(Collections.singletonList(newConfig));
        assertThat(validationErrors, hasSize(1));
        assertThat(validationErrors.get(0).getAttribute(), equalTo("frontend_multi_tenancy.default"));
        assertThat(validationErrors.get(0).getMessage(), equalTo("You try to enable multitenancy. This operation cannot be undone. Please use the 'sgctl.sh special enable-mt' command if you are sure that you want to proceed."));


        //should compare to default (disabled) configuration
        feMultiTenancyEnabledFlagValidator.setConfigMap(null);

        feMtConfig = FeMultiTenancyConfig.parse(DocNode.of("enabled", true), null).get();
        newConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", feMtConfig);
        validationErrors = feMultiTenancyEnabledFlagValidator.validateConfigs(Collections.singletonList(newConfig));
        assertThat(validationErrors, hasSize(1));
        assertThat(validationErrors.get(0).getAttribute(), equalTo("frontend_multi_tenancy.default"));
        assertThat(validationErrors.get(0).getMessage(), equalTo("You try to enable multitenancy. This operation cannot be undone. Please use the 'sgctl.sh special enable-mt' command if you are sure that you want to proceed."));
    }

    @Test
    public void configList_shouldValidateFeMtEnabledFlag_valueChanges_thereIsNoKibanaIndex() throws Exception {
        when(metadata.getProject(Metadata.DEFAULT_PROJECT_ID)).thenReturn(projectMetadata);
        when(projectMetadata.getIndicesLookup()).thenReturn(new TreeMap<>());

        //try to disable MT
        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE,
                        "default", FeMultiTenancyConfig.parse(DocNode.of("enabled", true), null).get())
        );
        feMultiTenancyEnabledFlagValidator.setConfigMap(configMap);

        FeMultiTenancyConfig feMtConfig = FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get();
        SgDynamicConfiguration<FeMultiTenancyConfig> newConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", feMtConfig);
        List<ValidationError> validationErrors = feMultiTenancyEnabledFlagValidator.validateConfigs(Collections.singletonList(newConfig));
        assertThat(validationErrors, hasSize(0));

        //try to enable MT
        configMap = configMapWithConfig(
                SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE,
                        "default", FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get())
        );
        feMultiTenancyEnabledFlagValidator.setConfigMap(configMap);

        feMtConfig = FeMultiTenancyConfig.parse(DocNode.of("enabled", true), null).get();
        newConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", feMtConfig);
        validationErrors = feMultiTenancyEnabledFlagValidator.validateConfigs(Collections.singletonList(newConfig));
        assertThat(validationErrors, hasSize(0));


        //should compare to default (disabled) configuration
        feMultiTenancyEnabledFlagValidator.setConfigMap(null);

        feMtConfig = FeMultiTenancyConfig.parse(DocNode.of("enabled", true), null).get();
        newConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", feMtConfig);
        validationErrors = feMultiTenancyEnabledFlagValidator.validateConfigs(Collections.singletonList(newConfig));
        assertThat(validationErrors, hasSize(0));
    }

    @Test
    public void configList_shouldValidateFeMtEnabledFlag_valueDoesNotChange() throws Exception {

        //MT disabled
        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE,
                        "default", FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get())
        );
        feMultiTenancyEnabledFlagValidator.setConfigMap(configMap);

        FeMultiTenancyConfig feMtConfig = FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get();
        SgDynamicConfiguration<FeMultiTenancyConfig> newConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", feMtConfig);
        List<ValidationError> validationErrors = feMultiTenancyEnabledFlagValidator
                .validateConfigs(Arrays.asList(newConfig, null));
        assertThat(validationErrors, hasSize(0));

        //should compare to default (disabled) configuration
        feMultiTenancyEnabledFlagValidator.setConfigMap(null);

        feMtConfig = FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get();
        newConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", feMtConfig);
        validationErrors = feMultiTenancyEnabledFlagValidator
                .validateConfigs(Arrays.asList(newConfig, SgDynamicConfiguration.empty(CType.ROLES)));
        assertThat(validationErrors, hasSize(0));

        //MT enabled
        configMap = configMapWithConfig(
                SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE,
                        "default", FeMultiTenancyConfig.parse(DocNode.of("enabled", true), null).get())
        );
        feMultiTenancyEnabledFlagValidator.setConfigMap(configMap);

        feMtConfig = FeMultiTenancyConfig.parse(DocNode.of("enabled", true), null).get();
        newConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "default", feMtConfig);
        validationErrors = feMultiTenancyEnabledFlagValidator.validateConfigs(Collections.singletonList(newConfig));
        assertThat(validationErrors, hasSize(0));

        verifyNoInteractions(metadata);
    }

    @Test
    public void configList_shouldDoNothing_listContainsConfigWithUnsupportedType() throws Exception {

        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE,
                        "default", FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get())
        );
        feMultiTenancyEnabledFlagValidator.setConfigMap(configMap);
        Role role = Role.parse(DocNode.of("cluster_permissions", "*"), null).get();
        SgDynamicConfiguration<Role> newConfig = SgDynamicConfiguration.of(CType.ROLES, "name", role);
        List<ValidationError> validationErrors = feMultiTenancyEnabledFlagValidator.validateConfigs(Collections.singletonList(newConfig));
        assertThat(validationErrors, hasSize(0));

        verifyNoInteractions(metadata);
    }

    @Test
    public void configList_shouldDoNothing_listIsNull() throws Exception {

        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE,
                        "default", FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get())
        );
        feMultiTenancyEnabledFlagValidator.setConfigMap(configMap);
        List<ValidationError> validationErrors = feMultiTenancyEnabledFlagValidator.validateConfigs(null);
        assertThat(validationErrors, hasSize(0));

        verifyNoInteractions(metadata);
    }

    @Test
    public void configList_shouldDoNothing_listContainsOnlyNullElement() throws Exception {

        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE,
                        "default", FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get())
        );
        feMultiTenancyEnabledFlagValidator.setConfigMap(configMap);
        List<ValidationError> validationErrors = feMultiTenancyEnabledFlagValidator.validateConfigs(Collections.singletonList(null));
        assertThat(validationErrors, hasSize(0));

        verifyNoInteractions(metadata);
    }

    @Test
    public void configList_shouldDoNothing_listContainsEmptyConfig() throws Exception {

        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE,
                        "default", FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get())
        );
        feMultiTenancyEnabledFlagValidator.setConfigMap(configMap);
        SgDynamicConfiguration<FeMultiTenancyConfig> newConfig = SgDynamicConfiguration.empty(FeMultiTenancyConfig.TYPE);
        List<ValidationError> validationErrors = feMultiTenancyEnabledFlagValidator.validateConfigs(Collections.singletonList(newConfig));
        assertThat(validationErrors, hasSize(0));

        verifyNoInteractions(metadata);
    }

    @Test
    public void configList_shouldDoNothing_listContainsConfigWithUnsupportedKey() throws Exception {

        ConfigMap configMap = configMapWithConfig(
                SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE,
                        "default", FeMultiTenancyConfig.parse(DocNode.of("enabled", false), null).get())
        );
        feMultiTenancyEnabledFlagValidator.setConfigMap(configMap);
        FeMultiTenancyConfig feMtConfig = FeMultiTenancyConfig.parse(DocNode.of("enabled", true), null).get();
        SgDynamicConfiguration<FeMultiTenancyConfig> newConfig = SgDynamicConfiguration.of(FeMultiTenancyConfig.TYPE, "unsupported", feMtConfig);
        List<ValidationError> validationErrors = feMultiTenancyEnabledFlagValidator.validateConfigs(Collections.singletonList(newConfig));
        assertThat(validationErrors, hasSize(0));

        verifyNoInteractions(metadata);
    }

    private ConfigMap configMapWithConfig(SgDynamicConfiguration<?> config) {
        return new ConfigMap.Builder("index").with(config).build();
    }
}
