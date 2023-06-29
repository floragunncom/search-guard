package com.floragunn.searchsupport.jobs.core;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchsupport.jobs.JobExecutionEngineTest.TestJob;
import com.floragunn.searchsupport.jobs.config.DefaultJobConfig;
import com.floragunn.searchsupport.jobs.config.DefaultJobConfigFactory;
import com.floragunn.searchsupport.jobs.config.JobConfigFactory;
import com.floragunn.searchsupport.jobs.config.JobTemplateInstanceFactory;
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
 * The test verifies if Job Instances are created from template is created by {@link JobTemplateInstanceFactory}.
 */
@RunWith(MockitoJUnitRunner.class)
public class IndexJobStateStoreTemplateTest {

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
    private JobTemplateInstanceFactory<DefaultJobConfig> jobTemplateInstanceFactory;
    @Mock
    private SchedulerSignaler schedulerSignaler;

    // under tests
    private IndexJobStateStore<DefaultJobConfig> indexJobStateStore;

    @Before
    public void before() {
        JobConfigFactory<DefaultJobConfig>jobConfigFactory = new DefaultJobConfigFactory(TestJob.class);
        this.jobConfigSource = new ArrayList<>();
        this.indexJobStateStore = new IndexJobStateStore<>(SCHEDULER_NAME, INDEX_NAME, STATUS_INDEX_ID_PREFIX,
            NODE_ID, client, jobConfigSource, jobConfigFactory, clusterService, Collections.emptyList(), jobTemplateInstanceFactory);
    }

    @Test
    public void shouldInvokeJobTemplateInstanceFactoryWhenInitialization() throws SchedulerConfigException, JobPersistenceException {
        DefaultJobConfig defaultJobConfig = createJobConfig(JOB_KEY_1);
        this.jobConfigSource.add(defaultJobConfig);
        when(jobTemplateInstanceFactory.instantiateTemplate(any())) //
            .thenAnswer((Answer<ImmutableList<DefaultJobConfig>>) invocation -> ImmutableList.of(invocation.<DefaultJobConfig>getArgument(0)));

        indexJobStateStore.initialize(null, schedulerSignaler);

        assertThat(indexJobStateStore.getNumberOfJobs(), equalTo(1));
        verify(jobTemplateInstanceFactory).instantiateTemplate(same(defaultJobConfig));
    }

    @Test
    public void shouldCreateJobInstancesWhenInitialization() throws SchedulerConfigException, JobPersistenceException {
        DefaultJobConfig defaultJobConfigTemplate = createJobConfig(JOB_KEY_1);
        DefaultJobConfig jobConfigInstance1 = createJobConfig(JOB_INSTANCE_KEY_1);
        DefaultJobConfig jobConfigInstance2 = createJobConfig(JOB_INSTANCE_KEY_2);
        this.jobConfigSource.add(defaultJobConfigTemplate);
        when(jobTemplateInstanceFactory.instantiateTemplate(same(defaultJobConfigTemplate)))//
            .thenReturn(ImmutableList.of(jobConfigInstance1, jobConfigInstance2));

        indexJobStateStore.initialize(null, schedulerSignaler);

        assertThat(indexJobStateStore.getNumberOfJobs(), equalTo(2));
        verify(jobTemplateInstanceFactory).instantiateTemplate(same(defaultJobConfigTemplate));

        assertThat(indexJobStateStore.checkExists(new JobKey(JOB_INSTANCE_KEY_1)), equalTo(true));
        assertThat(indexJobStateStore.checkExists(new JobKey(JOB_INSTANCE_KEY_2)), equalTo(true));
        assertThat(indexJobStateStore.checkExists(new JobKey(JOB_KEY_1)), equalTo(false));
    }

    @Test
    public void shouldInvokeJobTemplateInstanceFactoryWhenClusterConfigChange() throws JobPersistenceException {
        DefaultJobConfig defaultJobConfig = createJobConfig(JOB_KEY_1);
        this.jobConfigSource.add(defaultJobConfig);
        when(jobTemplateInstanceFactory.instantiateTemplate(any())) //
            .thenAnswer((Answer<ImmutableList<DefaultJobConfig>>) invocation -> ImmutableList.of(invocation.<DefaultJobConfig>getArgument(0)));

        indexJobStateStore.clusterConfigChanged(null);

        Awaitility.await().until(() -> indexJobStateStore.getNumberOfJobs() > 0);
        assertThat(indexJobStateStore.getNumberOfJobs(), equalTo(1));
        verify(jobTemplateInstanceFactory).instantiateTemplate(same(defaultJobConfig));
    }

    @Test
    public void shouldCreateJobInstancesWhenClusterConfigChange() throws JobPersistenceException {
        DefaultJobConfig defaultJobConfigTemplate = createJobConfig(JOB_KEY_1);
        DefaultJobConfig jobConfigInstance1 = createJobConfig(JOB_INSTANCE_KEY_1);
        DefaultJobConfig jobConfigInstance2 = createJobConfig(JOB_INSTANCE_KEY_2);
        this.jobConfigSource.add(defaultJobConfigTemplate);
        when(jobTemplateInstanceFactory.instantiateTemplate(same(defaultJobConfigTemplate)))
            .thenReturn(ImmutableList.of(jobConfigInstance1, jobConfigInstance2));

        indexJobStateStore.clusterConfigChanged(null);

        Awaitility.await().until(() -> indexJobStateStore.getNumberOfJobs() >= 2);
        assertThat(indexJobStateStore.getNumberOfJobs(), equalTo(2));
        assertThat(indexJobStateStore.checkExists(new JobKey(JOB_INSTANCE_KEY_1)), equalTo(true));
        assertThat(indexJobStateStore.checkExists(new JobKey(JOB_INSTANCE_KEY_2)), equalTo(true));
        assertThat(indexJobStateStore.checkExists(new JobKey(JOB_KEY_1)), equalTo(false));
    }

    @Test
    public void shouldInvokeJobTemplateInstanceFactoryWhenUpdateJobsIsInvoked() throws JobPersistenceException, SchedulerConfigException {
        indexJobStateStore.initialize(null, schedulerSignaler);
        assertThat(indexJobStateStore.getNumberOfJobs(), equalTo(0));
        DefaultJobConfig defaultJobConfig = createJobConfig(JOB_KEY_1);
        this.jobConfigSource.add(defaultJobConfig);
        when(jobTemplateInstanceFactory.instantiateTemplate(any())) //
            .thenAnswer((Answer<ImmutableList<DefaultJobConfig>>) invocation -> ImmutableList.of(invocation.<DefaultJobConfig>getArgument(0)));

        indexJobStateStore.updateJobs();

        assertThat(indexJobStateStore.getNumberOfJobs(), equalTo(1));
        verify(jobTemplateInstanceFactory).instantiateTemplate(same(defaultJobConfig));
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
        when(jobTemplateInstanceFactory.instantiateTemplate(same(defaultJobConfigTemplate))).thenReturn(jobInstances);

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