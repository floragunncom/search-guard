package com.floragunn.signals;

import java.util.Arrays;
import java.util.List;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.node.PluginAwareNode;
import org.elasticsearch.script.ScriptService;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import com.floragunn.searchguard.internalauthtoken.InternalAuthTokenProvider;
import com.floragunn.searchguard.user.User;
import com.floragunn.signals.watch.WatchBuilder;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.common.Ack;

public class SignalsTenantTest {

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().resources("sg_config/no-tenants")
            .nodeSettings("signals.enabled", true, "signals.index_names.log", "signals_main_log").build();

    private static ClusterService clusterService;
    private static NamedXContentRegistry xContentRegistry;
    private static NodeEnvironment nodeEnvironment;
    private static ScriptService scriptService;
    private static InternalAuthTokenProvider internalAuthTokenProvider;
    private static final User UHURA = new User("uhura", Arrays.asList("signals_admin", "all_access"), null);

    @BeforeClass
    public static void setupTestData() throws Exception {

        PluginAwareNode node = cluster.node();

        clusterService = node.injector().getInstance(ClusterService.class);
        xContentRegistry = node.injector().getInstance(NamedXContentRegistry.class);
        nodeEnvironment = node.injector().getInstance(NodeEnvironment.class);
        scriptService = node.injector().getInstance(ScriptService.class);
        internalAuthTokenProvider = node.injector().getInstance(InternalAuthTokenProvider.class);

        try (Client client = cluster.getInternalClient(); Client privilegedConfigClient = cluster.getPrivilegedConfigNodeClient()) {
            Watch watch = new WatchBuilder("test").cronTrigger("*/2 * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink").name("testsink").throttledFor("5s").build();

            watch.setTenant("test");

            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();

            watch.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);

            privilegedConfigClient.index(
                    new IndexRequest(".signals_watches").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(xContentBuilder).id("test/test_watch"))
                    .actionGet();

            client.index(new IndexRequest("testsource").source(XContentType.JSON, "key1", "val1", "key2", "val2")).actionGet();

            client.index(new IndexRequest("testsource").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "x", "b", "y"))
                    .actionGet();
            client.index(new IndexRequest("testsource").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "xx", "b", "yy"))
                    .actionGet();
        }

    }

    @Test
    public void initializationTest() throws Exception {

        try (Client client = cluster.getNodeClient()) {

            Settings settings = Settings.builder().build();

            try (SignalsTenant tenant = new SignalsTenant("test", client, clusterService, scriptService, xContentRegistry, nodeEnvironment,
                    internalAuthTokenProvider, new SignalsSettings(settings), null)) {
                tenant.init();

                Assert.assertEquals(1, tenant.getLocalWatchCount());
                Assert.assertTrue(tenant.runsWatchLocally("test_watch"));
            }
        }
    }

    @Test
    public void nodeFilterTest() throws Exception {

        try (Client client = cluster.getNodeClient()) {

            SignalsSettings settings = Mockito.mock(SignalsSettings.class, Mockito.RETURNS_DEEP_STUBS);
            Mockito.when(settings.getTenant("test").getNodeFilter()).thenReturn("unknown_attr:true");

            try (SignalsTenant tenant = new SignalsTenant("test", client, clusterService, scriptService, xContentRegistry, nodeEnvironment,
                    internalAuthTokenProvider, settings, null)) {
                tenant.init();

                Assert.assertEquals(0, tenant.getLocalWatchCount());
                Assert.assertFalse(tenant.runsWatchLocally("test_watch"));
            }
        }
    }

    @Test
    public void failoverTest() throws Exception {
        Ack ackedTime1;

        try (Client client = cluster.getNodeClientWithMockUser(UHURA)) {

            Settings settings = Settings.builder().build();

            try (SignalsTenant tenant = new SignalsTenant("failover_test", client, clusterService, scriptService, xContentRegistry, nodeEnvironment,
                    internalAuthTokenProvider, new SignalsSettings(settings), null)) {
                tenant.init();

                Watch watch = new WatchBuilder("test_watch").atInterval("100ms").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                        .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink").name("testsink").build();

                tenant.addWatch(watch, UHURA);

                Thread.sleep(100);

                Assert.assertEquals(1, tenant.getLocalWatchCount());
                Assert.assertTrue(tenant.runsWatchLocally("test_watch"));

                Thread.sleep(300);

                List<String> ackedActions = tenant.getWatchStateManager().getWatchState("test_watch").ack("horst");
                Assert.assertEquals(Arrays.asList("testsink"), ackedActions);

                ackedTime1 = tenant.getWatchStateManager().getWatchState("test_watch").getActionState("testsink").getAcked();

                Assert.assertNotNull(ackedTime1);
            }

            Thread.sleep(500);

            try (SignalsTenant tenant = new SignalsTenant("failover_test", client, clusterService, scriptService, xContentRegistry, nodeEnvironment,
                    internalAuthTokenProvider, new SignalsSettings(settings), null)) {
                tenant.init();

                Thread.sleep(100);

                Assert.assertEquals(1, tenant.getLocalWatchCount());
                Assert.assertTrue(tenant.runsWatchLocally("test_watch"));

                Ack ackedTime2 = tenant.getWatchStateManager().getWatchState("test_watch").getActionState("testsink").getAcked();

                Assert.assertEquals(ackedTime1, ackedTime2);
            }
        }

    }
}
