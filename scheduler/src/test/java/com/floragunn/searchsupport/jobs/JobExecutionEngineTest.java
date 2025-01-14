package com.floragunn.searchsupport.jobs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.node.PluginAwareNode;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xcontent.XContentType;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchsupport.jobs.actions.SchedulerConfigUpdateAction;
import com.floragunn.searchsupport.jobs.cluster.NodeNameComparator;
import com.floragunn.searchsupport.jobs.config.DefaultJobConfig;

import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe
public class JobExecutionEngineTest {
    private static final Logger log = LogManager.getLogger(JobExecutionEngineTest.class);

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().nodeSettings("searchguard.unsupported.load_static_resources", false).build();

    @Test
    public void emptyNodeFilterTest() throws Exception {

        String test = "empty_node_filter";
        String jobConfigIndex = "test_job_config_" + test;

        Scheduler scheduler = null;

        try {
            Client tc = cluster.getInternalClient();
            String jobConfig = createIntervalJobConfig(1, "emptyNodeFilterTest", "100ms");

            tc.index(new IndexRequest(jobConfigIndex).setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(jobConfig, XContentType.JSON)).actionGet();

            PluginAwareNode node = cluster.node();

            ClusterService clusterService = node.injector().getInstance(ClusterService.class);
            NodeEnvironment nodeEnvironment = node.injector().getInstance(NodeEnvironment.class);

            scheduler = new SchedulerBuilder<DefaultJobConfig>().client(tc).name("test_" + test).nodeFilter("node_group_1:xxx")
                    .configIndex(jobConfigIndex).jobConfigFactory(new ConstantHashJobConfig.Factory(TestJob.class)).distributed(clusterService, nodeEnvironment)
                    .nodeComparator(new NodeNameComparator(clusterService)).build();

            scheduler.start();

            Thread.sleep(3 * 1000);

            int count = TestJob.getCounter("emptyNodeFilterTest");

            assertEquals(0, count);

        } finally {
            if (scheduler != null) {
                scheduler.shutdown();
            }
        }
    }
    

    /**
     * This test will create a trigger in a scheduler instance and verify that the execution of the trigger causes the startTime attribute to be properly written.
     * Afterwards, it will simulate a fail-over by stopping the scheduler and starting a new scheduler instance. Then, the new scheduler instance needs to properly
     * pick up the old startTime and follow the same rhythm.
     */
    @Test
    public void intervalTriggerStartTime() throws Exception {
        String test = "interval_trigger_start_time";
        String jobConfigIndex = "test_job_config_" + test;

        Scheduler scheduler = null;
        Client client = cluster.getInternalClient();
        
        try {
            String jobConfig = createIntervalJobConfig(1, "intervalTriggerStartTime", "100ms");

            client.index(new IndexRequest(jobConfigIndex).setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(jobConfig, XContentType.JSON)).actionGet();

            PluginAwareNode node = cluster.node();

            ClusterService clusterService = node.injector().getInstance(ClusterService.class);
            NodeEnvironment nodeEnvironment = node.injector().getInstance(NodeEnvironment.class);

            scheduler = new SchedulerBuilder<DefaultJobConfig>().client(client).name("test_" + test)
                    .configIndex(jobConfigIndex).jobConfigFactory(new ConstantHashJobConfig.Factory(TestJob.class)).distributed(clusterService, nodeEnvironment)
                    .nodeComparator(new NodeNameComparator(clusterService)).build();

            scheduler.start();

            Thread.sleep(2 * 1000);

           SearchResponse searchResponse = client.search(new SearchRequest("test_job_config_interval_trigger_start_time_trigger_state")).actionGet();

           System.out.println(Strings.toString(searchResponse));
           SearchHit hit = searchResponse.getHits().getAt(0);
           DocNode hitSource = DocNode.wrap(hit.getSourceAsMap());
           
           assertNotNull(hitSource + " must contain startTime attr", hitSource.get("startTime"));
               
           long startTime = hitSource.getNumber("startTime").longValue();
           long nextFireTime = hitSource.getNumber("nextFireTime").longValue();
           long timesTriggered =  hitSource.getNumber("timesTriggered").longValue();
           
           assertEquals("relationship between startTime " + startTime + ", nextFireTime" + nextFireTime + ", and timesTriggered " + timesTriggered, nextFireTime, startTime + 100 * timesTriggered);
           
           scheduler.shutdown();
           
           scheduler = new SchedulerBuilder<DefaultJobConfig>().client(client).name("test_" + test)
                   .configIndex(jobConfigIndex).jobConfigFactory(new ConstantHashJobConfig.Factory(TestJob.class)).distributed(clusterService, nodeEnvironment)
                   .nodeComparator(new NodeNameComparator(clusterService)).build();

           scheduler.start();
           
           Thread.sleep(2 * 1000);

           searchResponse = client.search(new SearchRequest("test_job_config_interval_trigger_start_time_trigger_state")).actionGet();

           System.out.println(Strings.toString(searchResponse));
            
           hit = searchResponse.getHits().getAt(0);
           hitSource = DocNode.wrap(hit.getSourceAsMap());           
                          
           long startTime2 = hitSource.getNumber("startTime").longValue();
           long nextFireTime2 = hitSource.getNumber("nextFireTime").longValue();
           long timesTriggered2 =  hitSource.getNumber("timesTriggered").longValue();
           
           assertNotEquals("Job was triggered", timesTriggered, timesTriggered2);

           assertEquals("startTime must have been preserved over scheduler restart", startTime, startTime2);
           
           assertEquals("relationship between startTime " + startTime2 + ", nextFireTime" + nextFireTime2 + ", and timesTriggered " + timesTriggered2, nextFireTime2, startTime2 + 100 * timesTriggered2);
           
            
        } finally {
            if (scheduler != null) {
                scheduler.shutdown();
            }
        }
    }
    
    
    
    @Ignore("Only for manual testing")
    @Test
    public void overCapacity() throws Exception {
        String test = "over_capacity";
        String jobConfigIndex = "test_job_config_" + test;

        Scheduler scheduler = null;
        
        JobFactory jobFactory = new JobFactory() {
            
            @Override
            public Job newJob(TriggerFiredBundle bundle, Scheduler scheduler) throws SchedulerException {
                return new LoggingTestJob();
            }
        };

        try {
            Client client = cluster.getInternalClient();
            client.index(new IndexRequest(jobConfigIndex).setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("job1").source(createIntervalJobConfig(1, "job1", "5000ms"), XContentType.JSON)).actionGet();
            client.index(new IndexRequest(jobConfigIndex).setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("job2").source(createIntervalJobConfig(1, "job2", "5000ms"), XContentType.JSON)).actionGet();
            client.index(new IndexRequest(jobConfigIndex).setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("job3").source(createIntervalJobConfig(1, "job3", "5000ms"), XContentType.JSON)).actionGet();
            client.index(new IndexRequest(jobConfigIndex).setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("job4").source(createIntervalJobConfig(1, "job4", "5000ms"), XContentType.JSON)).actionGet();
            client.index(new IndexRequest(jobConfigIndex).setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("job5").source(createIntervalJobConfig(1, "job5", "5000ms"), XContentType.JSON)).actionGet();


            PluginAwareNode node = cluster.node();

            ClusterService clusterService = node.injector().getInstance(ClusterService.class);
            NodeEnvironment nodeEnvironment = node.injector().getInstance(NodeEnvironment.class);

            scheduler = new SchedulerBuilder<DefaultJobConfig>().client(client).name("test_" + test).misfireThreshold(2000).maxThreads(1)
                    .configIndex(jobConfigIndex).jobConfigFactory(new ConstantHashJobConfig.Factory(TestJob.class)).jobFactory(jobFactory).distributed(clusterService, nodeEnvironment)
                    .nodeComparator(new NodeNameComparator(clusterService)).build();

            scheduler.start();

            Thread.sleep(30 * 1000);

            int count = TestJob.getCounter("emptyNodeFilterTest");

            assertEquals(0, count);

        } finally {
            if (scheduler != null) {
                scheduler.shutdown();
            }
        }
    }

    @Ignore("TODO why is this ignored?")
    @Test
    public void configUpdateTest() throws Exception {

        String test = "config_update";
        String jobConfigIndex = "test_job_config_" + test;

        Scheduler scheduler = null;

        try {
            Client tc = cluster.getInternalClient();
            String jobConfig = createIntervalJobConfig(1, "basic", "1200ms");

            tc.index(new IndexRequest(jobConfigIndex).setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(jobConfig, XContentType.JSON)).actionGet();

            PluginAwareNode node = cluster.node();

            ClusterService clusterService = node.injector().getInstance(ClusterService.class);
            NodeEnvironment nodeEnvironment = node.injector().getInstance(NodeEnvironment.class);

            scheduler = new SchedulerBuilder<DefaultJobConfig>().client(tc).name("test_" + test).configIndex(jobConfigIndex)
                    .jobConfigFactory(new ConstantHashJobConfig.Factory(TestJob.class)).distributed(clusterService, nodeEnvironment)
                    .nodeComparator(new NodeNameComparator(clusterService)).build();

            scheduler.start();

            Thread.sleep(500);

            jobConfig = createIntervalJobConfig(1, "late", "100ms");
            tc.index(new IndexRequest(jobConfigIndex).setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(jobConfig, XContentType.JSON)).actionGet();
            SchedulerConfigUpdateAction.send(tc, scheduler.getSchedulerName());

            Thread.sleep(3 * 1000);

            int count = TestJob.getCounter("late");

            assertTrue("count is " + count, count >= 1);

        } finally {
            if (scheduler != null) {
                scheduler.shutdown();
            }
        }
    }

    @Ignore("TODO why is this ignored?")
    @Test
    public void triggerUpdateTest() throws Exception {
        Client tc = cluster.getInternalClient();

        String jobConfig = createCronJobConfig(1, "basic", null, "*/1 * * * * ?");

        tc.index(new IndexRequest("testjobconfig").id("trigger_update_test_job").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(jobConfig,
                XContentType.JSON)).actionGet();

        PluginAwareNode node = cluster.node();

        ClusterService clusterService = node.injector().getInstance(ClusterService.class);
        NodeEnvironment nodeEnvironment = node.injector().getInstance(NodeEnvironment.class);

        Scheduler scheduler = new SchedulerBuilder<DefaultJobConfig>().client(tc).name("test").configIndex("testjobconfig")
                .nodeFilter("node_index:1").jobConfigFactory(new ConstantHashJobConfig.Factory(TestJob.class)).distributed(clusterService, nodeEnvironment)
                .nodeComparator(new NodeNameComparator(clusterService)).build();

        scheduler.start();

        Thread.sleep(3 * 1000);

        int count1 = TestJob.getCounter("basic");

        jobConfig = createCronJobConfig(1, "basic", null, "* * * * * ?");
        tc.index(new IndexRequest("testjobconfig").id("trigger_update_test_job").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(jobConfig,
                XContentType.JSON)).actionGet();
        SchedulerConfigUpdateAction.send(tc, "test");

        Thread.sleep(3 * 1000);

        int count2 = TestJob.getCounter("basic");

        //System.out.println("count1: " + count1 + "; count2: " + count2);

        assertTrue("count is " + count2, count2 > count1);


    }

    private String createIntervalJobConfig(int hash, String name, String interval) {
        StringBuilder result = new StringBuilder("{");

        result.append("\"hash\": ").append(hash).append(",");

        if (name != null) {
            result.append("\"name\": \"").append(name).append("\",");
        }

        result.append("\"trigger\": {\"schedule\": {\"interval\": ");

        result.append("\"").append(interval).append("\"");

        result.append("}}}");

        return result.toString();
    }

    private String createCronJobConfig(int hash, String name, Integer delay, String... cronSchedule) {
        StringBuilder result = new StringBuilder("{");

        result.append("\"hash\": ").append(hash).append(",");

        if (name != null) {
            result.append("\"name\": \"").append(name).append("\",");
        }

        if (delay != null) {
            result.append("\"delay\": ").append(delay).append(",");
        }

        result.append("\"trigger\": {\"schedule\": {\"cron\": ");

        if (cronSchedule.length == 1) {
            result.append("\"").append(cronSchedule[0]).append("\"");
        } else {
            result.append("[");
            boolean first = true;
            for (String cron : cronSchedule) {
                if (first) {
                    first = false;
                } else {
                    result.append(",");
                }
                result.append("\"").append(cron).append("\"");

            }
            result.append("]");
        }

        result.append("}}}");

        return result.toString();
    }

    public static class LoggingTestJob implements Job {

        
        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            try {
                log.info("execute: " + context.getJobDetail().getKey().getName() + ": scheduled: " + context.getScheduledFireTime());              
                Thread.sleep(2500);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    
    public static class TestJob implements Job {

        static Map<String, Integer> counters = new ConcurrentHashMap<>();

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            String name = context.getMergedJobDataMap().getString("name");

            if (name != null) {
                incrementCounter(name);
                int maxConcurrency = incrementCounter(name + "_active_concurrent");

                if (maxConcurrency > getCounter(name + "_max_concurrency")) {
                    setCounter(name + "_max_concurrency", maxConcurrency);
                }

                log.info("JOB " + name + " #" + getCounter(name));
            }

            Number delay = (Number) context.getMergedJobDataMap().get("delay");

            if (delay != null) {
                try {
                    Thread.sleep(delay.longValue());
                } catch (InterruptedException e) {

                }
            }

            decrementCounter(name + "_active_concurrent");
        }

        static int incrementCounter(String counterName) {
            int value = getCounter(counterName) + 1;
            counters.put(counterName, value);
            return value;
        }

        static int decrementCounter(String counterName) {
            int value = getCounter(counterName) - 1;
            counters.put(counterName, value);
            return value;
        }

        static void setCounter(String counterName, int number) {
            counters.put(counterName, number);
        }

        static int getCounter(String counterName) {
            Integer value = counters.get(counterName);

            if (value == null) {
                return 0;
            } else {
                return value;
            }
        }
    }

    @DisallowConcurrentExecution
    public static class NonConcurrentTestJob extends TestJob {
        int prevMaxConcurrency = 0;

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            String name = context.getMergedJobDataMap().getString("name");

            super.execute(context);

            if (name != null) {
                int maxConcurrency = getCounter(name + "_max_concurrency");

                if (maxConcurrency > prevMaxConcurrency && maxConcurrency > 1) {
                    log.error("DisallowConcurrentExecution constraint violated during last job run of " + name + " " + maxConcurrency + " ("
                            + prevMaxConcurrency + ")");
                    prevMaxConcurrency = maxConcurrency;
                }

            }

            log.info("JOB " + name + " finished");
        }

    }

}
