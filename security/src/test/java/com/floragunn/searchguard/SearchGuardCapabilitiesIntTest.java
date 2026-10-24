/*
 * Copyright 2022 floragunn GmbH
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

import java.util.Collections;

import org.elasticsearch.cluster.service.ClusterService;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

public class SearchGuardCapabilitiesIntTest {

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().sslEnabled().embedded().build();
    
    @Test
    public void basicTest() {
        ClusterService clusterService = cluster.getInjectable(ClusterService.class);
        
        SearchGuardCapabilities capabilities = new SearchGuardCapabilities(Collections.emptyList(), clusterService, cluster.getInternalNodeClient());

        // The capabilities of the Search Guard core are present even if no module provides capabilities
        assertThat(capabilities.getCapabilities(), hasItem(SearchGuardCapabilities.ESQL_DATA_NODE_AUTHZ));

        // This instance has just been created, so it has not retrieved the capabilities of the other nodes of the cluster
        // yet. A capability must not be reported as supported by all nodes as long as the capabilities of a node are
        // unknown, as features which require the support of all nodes would be used too early.
        assertThat(capabilities.hasCapability(SearchGuardCapabilities.ESQL_DATA_NODE_AUTHZ), is(false));
    }

    /**
     * Features which require all nodes to support them check the capabilities of the instance which the plugin provides,
     * see SearchGuardFilter and the ES|QL authorization.
     */
    @Test
    public void coreCapabilitiesOfProvidedInstance() {
        SearchGuardCapabilities capabilities = cluster.getInjectable(SearchGuardCapabilities.class);

        assertThat(capabilities.hasCapability(SearchGuardCapabilities.ESQL_DATA_NODE_AUTHZ), is(true));
        assertThat(capabilities.getCapabilities(), hasItem(SearchGuardCapabilities.ESQL_DATA_NODE_AUTHZ));
    }
}
