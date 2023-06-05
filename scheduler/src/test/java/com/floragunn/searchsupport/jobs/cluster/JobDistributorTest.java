/*
 * Copyright 2019-2023 floragunn GmbH
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
package com.floragunn.searchsupport.jobs.cluster;

import com.floragunn.searchsupport.jobs.config.JobConfig;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.quartz.Job;
import org.quartz.JobKey;
import org.quartz.Trigger;

import java.util.Collection;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class JobDistributorTest {

    public static final String EMPTY_NODE_FILTER = "";
    public static final String LOCAL_NODE_ID = "local-node-id";
    public static final String OTHER_NODE_ID = "other-node-id";
    public static final String[] AVAILABLE_NODES = { LOCAL_NODE_ID, OTHER_NODE_ID };
    @Mock
    private DistributedJobStore jobStore;

    @Mock
    private ClusterService clusterService;

    @Mock
    private NodeComparator<Object> nodeComparator;

    @Mock
    private ClusterState clusterState;

    @Mock
    private DiscoveryNodes discoveryNodes;

    private JobDistributor jobDistributor;

    @Before
    public void before() {
        when(nodeComparator.resolveNodeFilters(any())).thenReturn(AVAILABLE_NODES);
        when(nodeComparator.resolveNodeId(LOCAL_NODE_ID)).thenReturn( LOCAL_NODE_ID );
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.nodes()).thenReturn(discoveryNodes);
        when(discoveryNodes.getLocalNodeId()).thenReturn(LOCAL_NODE_ID);
        this.jobDistributor = new JobDistributor("name", EMPTY_NODE_FILTER, clusterService, jobStore, nodeComparator);
        // job jobDistributor should contain at this stage
        // - availableNodes = 2
        // - current node = 0
    }

    @Test
    public void shouldExecuteJobWithHashCodeZeroOnCurrentNode() {
        // current node is 0
        // should not execute on current node because
        // jobConfig.hashCode() % AVAILABLE_NODES.length = 0
        // 2 % 2 = 0
        TestJobConfig jobConfig = new TestJobConfig(2, false);

        // so that every node will try to create instances of generic watches, and then execute only proper instances
        boolean jobSelected = jobDistributor.isJobSelected(jobConfig);

        assertThat(jobSelected, equalTo(true));
    }

    @Test
    public void shouldNotExecuteJobWithHashCodeOneOnCurrentNode() {
        // current node is 0
        // should not execute on current node because
        // jobConfig.hashCode() % AVAILABLE_NODES.length = 1
        // 1 % 2 = 1
        TestJobConfig jobConfig = new TestJobConfig(1, false);

        // so that every node will try to create instances of generic watches, and then execute only proper instances
        boolean jobSelected = jobDistributor.isJobSelected(jobConfig);

        assertThat(jobSelected, equalTo(false));
    }

    @Test
    public void shouldExecuteGenericJobWithHashCodeZeroOnCurrentNode() {
        // generic job is always executed on current node
        TestJobConfig jobConfig = new TestJobConfig(2, true);

        // so that every node will try to create instances of generic watches, and then execute only proper instances
        boolean jobSelected = jobDistributor.isJobSelected(jobConfig);

        assertThat(jobSelected, equalTo(true));
    }

    @Test
    public void shouldExecuteGenericJobWithHashCodeOneOnCurrentNode() {
        // generic job is always executed on current node
        TestJobConfig jobConfig = new TestJobConfig(1, true);

        // so that every node will try to create instances of generic watches, and then execute only proper instances
        boolean jobSelected = jobDistributor.isJobSelected(jobConfig);

        assertThat(jobSelected, equalTo(true));
    }

    /**
     * Unfortunately mockito is not able to stub hashCode method, so test implementation is needed
     */
    private static class TestJobConfig implements JobConfig {

        private final int hash;
        private final boolean genericJob;

        public TestJobConfig(int hash, boolean genericJob) {
            this.hash = hash;
            this.genericJob = genericJob;
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public JobKey getJobKey() {
            return null;
        }

        @Override
        public String getDescription() {
            return null;
        }

        @Override
        public Class<? extends Job> getJobClass() {
            return null;
        }

        @Override
        public Map<String, Object> getJobDataMap() {
            return null;
        }

        @Override
        public boolean isDurable() {
            return false;
        }

        @Override
        public Collection<Trigger> getTriggers() {
            return null;
        }

        @Override
        public long getVersion() {
            return 0;
        }

        @Override
        public String getAuthToken() {
            return null;
        }

        @Override
        public String getSecureAuthTokenAudience() {
            return null;
        }

        @Override
        public boolean isExecutable() {
            return false;
        }

        @Override
        public boolean isGenericJobConfig() {
            return genericJob;
        }
    }

}