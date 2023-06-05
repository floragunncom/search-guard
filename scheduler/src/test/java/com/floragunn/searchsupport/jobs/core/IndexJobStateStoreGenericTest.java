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
package com.floragunn.searchsupport.jobs.core;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchsupport.jobs.JobExecutionEngineTest.TestJob;
import com.floragunn.searchsupport.jobs.config.DefaultJobConfig;
import com.floragunn.searchsupport.jobs.config.DefaultJobConfigFactory;
import com.floragunn.searchsupport.jobs.config.JobConfigFactory;
import com.floragunn.searchsupport.jobs.config.GenericJobInstanceFactory;
import org.awaitility.Awaitility;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.SchedulerConfigException;
import org.quartz.spi.SchedulerSignaler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The test verifies if Job Instances are created from generic jobs by {@link GenericJobInstanceFactory}.
 */
@RunWith(MockitoJUnitRunner.class)
public class IndexJobStateStoreGenericTest {

    public static final String JOB_KEY_1 = "job-key-1";
    public static final String JOB_INSTANCE_KEY_1 = "job-instance-1";
    public static final String JOB_INSTANCE_KEY_2 = "job-instance-2";
    public static final String SCHEDULER_NAME = "scheduler-name";
    public static final String INDEX_NAME = "index-name";
    public static final String STATUS_INDEX_ID_PREFIX = "status-index-id-prefix";
    public static final String NODE_ID = "node-id";

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Client client;
    private List<DefaultJobConfig> jobConfigSource;
    @Mock
    private ClusterService clusterService;
    @Mock
    private GenericJobInstanceFactory<DefaultJobConfig> genericJobInstanceFactory;
    @Mock
    private SchedulerSignaler schedulerSignaler;

    // under tests
    private IndexJobStateStore<DefaultJobConfig> indexJobStateStore;

    @Before
    public void before() {
        JobConfigFactory<DefaultJobConfig>jobConfigFactory = new DefaultJobConfigFactory(TestJob.class);
        this.jobConfigSource = new ArrayList<>();
        this.indexJobStateStore = new IndexJobStateStore<>(SCHEDULER_NAME, INDEX_NAME, STATUS_INDEX_ID_PREFIX,
            NODE_ID, client, jobConfigSource, jobConfigFactory, clusterService, Collections.emptyList(), genericJobInstanceFactory);
    }

    @Test
    public void shouldInvokeGenericJobInstanceFactoryDuringInitialization() throws SchedulerConfigException, JobPersistenceException {
        DefaultJobConfig defaultJobConfig = createJobConfig(JOB_KEY_1);
        this.jobConfigSource.add(defaultJobConfig);
        when(genericJobInstanceFactory.instantiateGeneric(any())) //
            .thenAnswer((Answer<ImmutableList<DefaultJobConfig>>) invocation -> ImmutableList.of(invocation.<DefaultJobConfig>getArgument(0)));

        indexJobStateStore.initialize(null, schedulerSignaler);

        assertThat(indexJobStateStore.getNumberOfJobs(), equalTo(1));
        verify(genericJobInstanceFactory).instantiateGeneric(same(defaultJobConfig));
    }

    @Test
    public void shouldCreateJobInstancesWhenInitialization() throws SchedulerConfigException, JobPersistenceException {
        DefaultJobConfig defaultJobConfigGeneric = createJobConfig(JOB_KEY_1);
        DefaultJobConfig jobConfigInstance1 = createJobConfig(JOB_INSTANCE_KEY_1);
        DefaultJobConfig jobConfigInstance2 = createJobConfig(JOB_INSTANCE_KEY_2);
        this.jobConfigSource.add(defaultJobConfigGeneric);
        when(genericJobInstanceFactory.instantiateGeneric(same(defaultJobConfigGeneric)))//
            .thenReturn(ImmutableList.of(jobConfigInstance1, jobConfigInstance2));

        indexJobStateStore.initialize(null, schedulerSignaler);

        assertThat(indexJobStateStore.getNumberOfJobs(), equalTo(2));
        verify(genericJobInstanceFactory).instantiateGeneric(same(defaultJobConfigGeneric));

        assertThat(indexJobStateStore.checkExists(new JobKey(JOB_INSTANCE_KEY_1)), equalTo(true));
        assertThat(indexJobStateStore.checkExists(new JobKey(JOB_INSTANCE_KEY_2)), equalTo(true));
        assertThat(indexJobStateStore.checkExists(new JobKey(JOB_KEY_1)), equalTo(false));
    }

    @Test
    public void shouldInvokeGenericJobInstanceFactoryWhenClusterConfigChange() throws JobPersistenceException {
        DefaultJobConfig defaultJobConfig = createJobConfig(JOB_KEY_1);
        this.jobConfigSource.add(defaultJobConfig);
        when(genericJobInstanceFactory.instantiateGeneric(any())) //
            .thenAnswer((Answer<ImmutableList<DefaultJobConfig>>) invocation -> ImmutableList.of(invocation.<DefaultJobConfig>getArgument(0)));

        indexJobStateStore.clusterConfigChanged(null);

        Awaitility.await().until(() -> indexJobStateStore.getNumberOfJobs() > 0);
        assertThat(indexJobStateStore.getNumberOfJobs(), equalTo(1));
        verify(genericJobInstanceFactory).instantiateGeneric(same(defaultJobConfig));
    }

    @Test
    public void shouldCreateJobInstancesWhenClusterConfigChange() throws JobPersistenceException {
        DefaultJobConfig defaultJobConfigGeneric = createJobConfig(JOB_KEY_1);
        DefaultJobConfig jobConfigInstance1 = createJobConfig(JOB_INSTANCE_KEY_1);
        DefaultJobConfig jobConfigInstance2 = createJobConfig(JOB_INSTANCE_KEY_2);
        this.jobConfigSource.add(defaultJobConfigGeneric);
        when(genericJobInstanceFactory.instantiateGeneric(same(defaultJobConfigGeneric)))
            .thenReturn(ImmutableList.of(jobConfigInstance1, jobConfigInstance2));

        indexJobStateStore.clusterConfigChanged(null);

        Awaitility.await().until(() -> indexJobStateStore.getNumberOfJobs() >= 2);
        assertThat(indexJobStateStore.getNumberOfJobs(), equalTo(2));
        assertThat(indexJobStateStore.checkExists(new JobKey(JOB_INSTANCE_KEY_1)), equalTo(true));
        assertThat(indexJobStateStore.checkExists(new JobKey(JOB_INSTANCE_KEY_2)), equalTo(true));
        assertThat(indexJobStateStore.checkExists(new JobKey(JOB_KEY_1)), equalTo(false));
    }

    @Test
    public void shouldInvokeGenericJobInstanceFactoryWhenUpdateJobsIsInvoked() throws JobPersistenceException, SchedulerConfigException {
        indexJobStateStore.initialize(null, schedulerSignaler);
        assertThat(indexJobStateStore.getNumberOfJobs(), equalTo(0));
        DefaultJobConfig defaultJobConfig = createJobConfig(JOB_KEY_1);
        this.jobConfigSource.add(defaultJobConfig);
        when(genericJobInstanceFactory.instantiateGeneric(any())) //
            .thenAnswer((Answer<ImmutableList<DefaultJobConfig>>) invocation -> ImmutableList.of(invocation.<DefaultJobConfig>getArgument(0)));

        indexJobStateStore.updateJobs();

        assertThat(indexJobStateStore.getNumberOfJobs(), equalTo(1));
        verify(genericJobInstanceFactory).instantiateGeneric(same(defaultJobConfig));
    }

    @Test
    public void shouldCreateJobInstanceWhenUpdateJobsIsInvoked() throws JobPersistenceException, SchedulerConfigException {
        indexJobStateStore.initialize(null, schedulerSignaler);
        assertThat(indexJobStateStore.getNumberOfJobs(), equalTo(0));
        DefaultJobConfig defaultJobConfigTemplate = createJobConfig(JOB_KEY_1);
        DefaultJobConfig jobConfigInstance1 = createJobConfig(JOB_INSTANCE_KEY_1);
        DefaultJobConfig jobConfigInstance2 = createJobConfig(JOB_INSTANCE_KEY_2);
        this.jobConfigSource.add(defaultJobConfigTemplate);
        ImmutableList<DefaultJobConfig> jobInstances = ImmutableList.of(jobConfigInstance1, jobConfigInstance2);
        when(genericJobInstanceFactory.instantiateGeneric(same(defaultJobConfigTemplate))).thenReturn(jobInstances);

        indexJobStateStore.updateJobs();

        assertThat(indexJobStateStore.getNumberOfJobs(), equalTo(2));
        assertThat(indexJobStateStore.checkExists(new JobKey(JOB_INSTANCE_KEY_1)), equalTo(true));
        assertThat(indexJobStateStore.checkExists(new JobKey(JOB_INSTANCE_KEY_2)), equalTo(true));
        assertThat(indexJobStateStore.checkExists(new JobKey(JOB_KEY_1)), equalTo(false));
    }


    private static DefaultJobConfig createJobConfig(String jobKey) {
        DefaultJobConfig defaultJobConfig = new DefaultJobConfig(TestJob.class);
        defaultJobConfig.setJobKey(new JobKey(jobKey));
        defaultJobConfig.setTriggers(ImmutableList.empty());
        return defaultJobConfig;
    }
}