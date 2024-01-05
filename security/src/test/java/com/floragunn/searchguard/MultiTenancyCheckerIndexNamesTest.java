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
import junit.framework.TestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class MultiTenancyCheckerIndexNamesTest extends TestCase {

    private final String indexName;
    private final boolean multiTenancyRelated;

    public MultiTenancyCheckerIndexNamesTest(String indexName, boolean multiTenancyRelated) {
        this.indexName = indexName;
        this.multiTenancyRelated = multiTenancyRelated;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            { ".kibana-event-log-7.17.12-000001", false },
            { ".kibana_1329513022_ittenant_7.17.12_001", true },
            { ".kibana_3292183_kirk_7.17.12_001", true },
            { ".signals_truststores", false },
            { ".ds-.logs-deprecation.elasticsearch-default-2023.09.14-000001", false },
            { ".kibana_8.7.0_001", false },
            { ".one_hundred", false },
            { "iot", false },
            { "generated_logs", false },
            { ".geoip_databases", false },
            { ".apm-custom-link", false },
            { ".kibana-event-log-8.7.0-000001", false },
            { ".kibana_3292183_kirk_8.7.0_001", true },
            { ".kibana_-152937574_admintenant_7.17.12_001", true },
            { ".kibana_-152937574_admintenant_8.7.0_001", true },
            { ".kibana_1329513022_ittenant_8.7.0_001", true },
            { ".kibana_1329513022_ittenant_8.7.0_002", true },
            { ".kibana_1329513022_ittenant_8.8.0_001", true },
            { ".kibana_1329513022_ittenant_8.11.3_001", true },
            { ".kibana_1329513022_ittenant_8.11.3_999", true },
            { ".kibana_109651354_spock_8.7.0_001", true },
            { "apm-agent-configuration", false },
            { ".apm-source-map", false },
            { ".kibana_92668751_admin_8.7.0_001", true },
            { ".kibana_739956815_uksz_8.7.0_001", true },
            { ".kibana_task_manager_8.7.0_001", false },
            { ".kibana_-348653185_hrtenant_7.17.12_001", true },
            { ".kibana_task_manager_7.17.12_001", false },
            { ".tasks", false },
            { ".kibana_-348653185_hrtenant_8.7.0_001", true },
            { ".kibana_-348653185_hrtenant_18.17.103_001", true },
            { ".kibana_-348653185_hrtenant_18.17.103_0012", false },
            { ".kibana_-348653185_hrtenant_8.7.0_001_backup", false },
            { ".kibana_-348653185_hrtenant_8.7.0_001_2", false },
            { ".kibana_-348653185_hrtenant_8.7.0_001_2023_12_11", false },
            { ".kibana_109651354_spock_7.17.12_001", true },
            { ".kibana_8.17.12_001", false },
            { ".kibana_8.9.12_001", false },
            { ".kibana_7.17.12_123", false },
            { ".kibana_739956815_uksz_7.17.12_001", true },
            { ".kibana_92668751_admin_7.17.12_001", true },
            { ".internal.alerts-observability.logs.alerts-default-000001", false },
            { ".ds-ilm-history-5-2023.12.12-000001", false },
            { ".kibana_security_solution_8.8.0_001", false },
            { ".internal.alerts-observability.metrics.alerts-default-000001", false },
            { ".kibana-event-log-8.8.0-000001 ", false },
            { ".internal.alerts-observability.apm.alerts-default-000001", false },
            { ".internal.alerts-observability.slo.alerts-default-000001", false },
            { ".kibana_ingest_8.8.0_001", false },
            { ".kibana_analytics_8.8.0_001", false },
            { ".apm-agent-configuration", false },
            { ".kibana_task_manager_8.8.0_001", false },
            { ".internal.alerts-observability.uptime.alerts-default-000001", false },
            { ".async-search", false },
            { ".async-search", false }
        });
    }

    @Test
    public void shouldDetermineIfIndexIsRelatedToMultiTenancy() {
        IndexRepository indexRepository = Mockito.mock(IndexRepository.class);
        MultiTenancyChecker checker = new MultiTenancyChecker(Settings.builder().build(), indexRepository);
        IndexMetadata mock = Mockito.mock(IndexMetadata.class);
        Mockito.when(mock.getCreationVersion()).thenReturn(IndexVersions.V_8_3_0);
        ImmutableMap<String, IndexMetadata> indices = ImmutableMap.of(indexName, mock);
        when(indexRepository.findIndicesMetadata()).thenReturn(indices);

        boolean currentIndexIsMultiTenancyRelated = checker.findMultiTenancyConfigurationError().isPresent();

        String reason = "Index " + indexName + " should be MT related " + this.multiTenancyRelated;
        assertThat(reason,  currentIndexIsMultiTenancyRelated, equalTo(this.multiTenancyRelated));
    }
}