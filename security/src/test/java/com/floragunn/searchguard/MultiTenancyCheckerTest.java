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
package com.floragunn.searchguard;

import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.MultiTenancyChecker.IndexRepository;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MultiTenancyCheckerTest  {

    @Mock
    private IndexRepository repository;

    // under tests
    private MultiTenancyChecker checker;

    @Before
    public void before() {
        this.checker = new MultiTenancyChecker(Settings.builder().build(), repository);
    }

    @Test
    public void shouldReportErrorWhenMultiTenancyIndicesArePresent() {
        ImmutableMap<String, IndexMetadata> indices = ImmutableMap
            .of(".kibana_-152937574_admintenant_7.17.12_001", mockMetadata(IndexVersion.V_8_3_0));
        when(repository.findIndicesMetadata()).thenReturn(indices);

        Optional<String> errorDescription =  checker.findMultiTenancyConfigurationError();

        assertThat(errorDescription.isPresent(), equalTo(true));
    }

    @Test
    public void shouldReportErrorWhenMultiTenancyIndicesArePresentAndBootstrapChecksAreExplicitelyEnabled() {
        Settings settings = Settings.builder().put("searchguard.multi_tenancy_bootstrap_check_enabled", true).build();
        ImmutableMap<String, IndexMetadata> indices = ImmutableMap
            .of(".kibana_-152937574_admintenant_7.17.12_001", mockMetadata(IndexVersion.V_8_3_0));
        this.checker = new MultiTenancyChecker(settings, repository);
        when(repository.findIndicesMetadata()).thenReturn(indices);

        Optional<String> errorDescription = checker.findMultiTenancyConfigurationError();

        assertThat(errorDescription.isPresent(), equalTo(true));
    }

    @Test
    public void shouldReportErrorWhenOnlyOneMultiTenancyRelatedIndexExist() {
        ImmutableMap<String, IndexMetadata> indices = ImmutableMap.of("data_index_1", mockMetadata(IndexVersion.V_8_5_0),
                "data_index_2", mockMetadata(IndexVersion.V_8_1_0),
                "logs_2023", mockMetadata(IndexVersion.V_8_2_0),
                ".kibana_92668751_admin_8.7.12_100", mockMetadata(IndexVersion.V_8_3_0)) //
            .with("logs_2024", mockMetadata(IndexVersion.V_8_4_0)) //
            .with("logs_2024_01_01", mockMetadata(IndexVersion.V_8_6_0));
        when(repository.findIndicesMetadata()).thenReturn(indices);

        Optional<String> errorDescription = checker.findMultiTenancyConfigurationError();

        assertThat(errorDescription.isPresent(), equalTo(true));
    }

    @Test
    public void shouldReportErrorWhenManyMultiTenancyRelatedIndexExist() {
        ImmutableMap<String, IndexMetadata> indices = ImmutableMap.of("data_index_1", mockMetadata(IndexVersion.V_8_5_0),
            "data_index_2", mockMetadata(IndexVersion.V_8_1_0),
            "logs_2023", mockMetadata(IndexVersion.V_8_2_0),
            ".kibana_92668751_admin_8.7.12_100", mockMetadata(IndexVersion.V_8_3_0)) //
            .with("logs_2024", mockMetadata(IndexVersion.V_8_4_0)) //
            .with(".kibana_-152937574_admintenant_7.17.12_001", mockMetadata(IndexVersion.V_8_5_0)) //
            .with(".kibana_1329513022_ittenant_8.7.0_002", mockMetadata(IndexVersion.V_8_6_0)) //
            .with("logs_2024_01_01", mockMetadata(IndexVersion.V_8_6_0));
        when(repository.findIndicesMetadata()).thenReturn(indices);

        Optional<String> errorDescription = checker.findMultiTenancyConfigurationError();

        assertThat(errorDescription.isPresent(), equalTo(true));
    }

    @Test
    public void shouldNotReportErrorWhenManyMultiTenancyRelatedIndexExistButWasCreatedInVersion8_8_0() {
        ImmutableMap<String, IndexMetadata> indices = ImmutableMap.of("data_index_1", mockMetadata(IndexVersion.V_8_5_0),
                "data_index_2", mockMetadata(IndexVersion.V_8_1_0),
                "logs_2023", mockMetadata(IndexVersion.V_8_2_0),
                ".kibana_92668751_admin_8.7.12_100", mockMetadata(IndexVersion.V_8_8_0)) //
            .with("logs_2024", mockMetadata(IndexVersion.V_8_4_0)) //
            .with(".kibana_-152937574_admintenant_7.17.12_001", mockMetadata(IndexVersion.V_8_8_0)) //
            .with(".kibana_1329513022_ittenant_8.7.0_002", mockMetadata(IndexVersion.V_8_8_0)) //
            .with("logs_2024_01_01", mockMetadata(IndexVersion.V_8_6_0));
        when(repository.findIndicesMetadata()).thenReturn(indices);

        Optional<String> errorDescription = checker.findMultiTenancyConfigurationError();

        assertThat(errorDescription.isPresent(), equalTo(false));
    }

    @Test
    public void shouldReportErrorWhenOneMultiTenancyRelatedIndexExistAndWasCreatedInVersionPriorTo8_8_0() {
        ImmutableMap<String, IndexMetadata> indices = ImmutableMap.of("data_index_1", mockMetadata(IndexVersion.V_8_5_0),
                "data_index_2", mockMetadata(IndexVersion.V_8_1_0),
                "logs_2023", mockMetadata(IndexVersion.V_8_2_0),
                ".kibana_92668751_admin_8.7.12_100", mockMetadata(IndexVersion.V_8_8_0)) //
            .with("logs_2024", mockMetadata(IndexVersion.V_8_4_0)) //
            .with(".kibana_-152937574_admintenant_7.17.12_001", mockMetadata(IndexVersion.V_8_7_0)) // this index cause error
            .with(".kibana_1329513022_ittenant_8.7.0_002", mockMetadata(IndexVersion.V_8_8_0)) //
            .with("logs_2024_01_01", mockMetadata(IndexVersion.V_8_6_0));
        when(repository.findIndicesMetadata()).thenReturn(indices);

        Optional<String> errorDescription = checker.findMultiTenancyConfigurationError();

        assertThat(errorDescription.isPresent(), equalTo(true));
    }

    @Test
    public void shouldDisableCheckingOfMultiTenancyRelatedIndices() {
        Settings settings = Settings.builder().put("searchguard.multi_tenancy_bootstrap_check_enabled", false).build();
        this.checker = new MultiTenancyChecker(settings, repository);

        Optional<String> errorDescription = checker.findMultiTenancyConfigurationError();

        assertThat(errorDescription.isPresent(), equalTo(false));
        verifyNoInteractions(repository);
    }

    @Test
    public void shouldNotReportErrorWhenMainKibanaIndexExist_1() {
        ImmutableMap<String, IndexMetadata> indexMetadata = ImmutableMap.of(".kibana_7.17.12_001", mockMetadata(IndexVersion.V_8_1_0));
        when(repository.findIndicesMetadata()).thenReturn(indexMetadata);

        Optional<String> errorDescription = checker.findMultiTenancyConfigurationError();

        assertThat(errorDescription.isPresent(), equalTo(false));
    }

    @Test
    public void shouldNotReportErrorWhenMainKibanaIndexExist_2() {
        ImmutableMap<String, IndexMetadata> indexMetadata = ImmutableMap.of(".kibana_7.17.12_001", mockMetadata(IndexVersion.V_7_0_0),
            ".kibana_8.8.0_001", mockMetadata(IndexVersion.V_8_7_0));
        when(repository.findIndicesMetadata()).thenReturn(indexMetadata);

        Optional<String> errorDescription = checker.findMultiTenancyConfigurationError();

        assertThat(errorDescription.isPresent(), equalTo(false));
    }

    private IndexMetadata mockMetadata(IndexVersion version) {
        IndexMetadata mock = Mockito.mock(IndexMetadata.class);
        when(mock.getCreationVersion()).thenReturn(version);
        return mock;
    }

}