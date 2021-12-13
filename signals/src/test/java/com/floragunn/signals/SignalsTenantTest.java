package com.floragunn.signals;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.node.PluginAwareNode;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import com.floragunn.searchguard.internalauthtoken.InternalAuthTokenProvider;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.config.validation.ValidationErrors;
import com.floragunn.searchsupport.diag.DiagnosticContext;
import com.floragunn.searchsupport.util.temporal.DurationFormat;
import com.floragunn.signals.execution.ActionExecutionException;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.settings.SignalsSettings;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.WatchBuilder;
import com.floragunn.signals.watch.action.handlers.ActionExecutionResult;
import com.floragunn.signals.watch.action.handlers.ActionHandler;
import com.floragunn.signals.watch.common.Ack;
import com.floragunn.signals.watch.init.WatchInitializationService;

import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe
public class SignalsTenantTest {

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().resources("sg_config/no-tenants")
            .nodeSettings("signals.enabled", true, "signals.index_names.log", "signals_main_log", "searchguard.enterprise_modules_enabled", false)
            .build();
 
    private static ClusterService clusterService;
    private static NodeEnvironment nodeEnvironment;
    private static NamedXContentRegistry xContentRegistry;
    private static ScriptService scriptService;
    private static InternalAuthTokenProvider internalAuthTokenProvider;
    private static DiagnosticContext diagnosticContext;
    private static final User UHURA = User.forUser("uhura").backendRoles("signals_admin", "all_access").build();

    @BeforeClass
    public static void setupTestData() throws Exception {

        PluginAwareNode node = cluster.node();

        clusterService = node.injector().getInstance(ClusterService.class);
        xContentRegistry = node.injector().getInstance(NamedXContentRegistry.class);
        scriptService = node.injector().getInstance(ScriptService.class);
        internalAuthTokenProvider = node.injector().getInstance(InternalAuthTokenProvider.class);
        nodeEnvironment = node.injector().getInstance(NodeEnvironment.class);
        diagnosticContext = node.injector().getInstance(DiagnosticContext.class);

        try (Client client = cluster.getAdminCertClient();
                Client privilegedConfigClient = PrivilegedConfigClient.adapt(cluster.getInternalNodeClient())) {
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

    @Ignore
    @Test
    public void initializationTest() throws Exception {

        try (Client client = cluster.getInternalNodeClient()) {

            Settings settings = Settings.builder().build();

            try (SignalsTenant tenant = new SignalsTenant("test", client, clusterService, nodeEnvironment, scriptService, xContentRegistry,
                    internalAuthTokenProvider, new SignalsSettings(settings), null, diagnosticContext)) {
                tenant.init();

                Assert.assertEquals(1, tenant.getLocalWatchCount());
                Assert.assertTrue(tenant.runsWatchLocally("test_watch"));
            }
        }
    }

    @Test
    public void nodeFilterTest() throws Exception {

        try (Client client = cluster.getInternalNodeClient()) {

            SignalsSettings settings = Mockito.mock(SignalsSettings.class, Mockito.RETURNS_DEEP_STUBS);
            Mockito.when(settings.getTenant("test").getNodeFilter()).thenReturn("unknown_attr:true");

            try (SignalsTenant tenant = new SignalsTenant("test", client, clusterService, nodeEnvironment, scriptService, xContentRegistry,
                    internalAuthTokenProvider, settings, null, diagnosticContext)) {
                tenant.init();

                Assert.assertEquals(0, tenant.getLocalWatchCount());
                Assert.assertFalse(tenant.runsWatchLocally("test_watch"));
            }
        }
    }

    @Test
    public void failoverTest() throws Exception {
        Ack ackedTime1;

        try (Client client = cluster.getInternalNodeClient()) {

            Settings settings = Settings.builder().build();

            try (SignalsTenant tenant = new SignalsTenant("failover_test", client, clusterService, nodeEnvironment, scriptService, xContentRegistry,
                    internalAuthTokenProvider, new SignalsSettings(settings), null, diagnosticContext)) {
                tenant.init();

                Watch watch = new WatchBuilder("test_watch").atInterval("100ms").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                        .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink").name("testsink").build();

                tenant.addWatch(watch, UHURA);

                for (int i = 0; i < 20; i++) {
                    Thread.sleep(100);

                    if (tenant.getLocalWatchCount() != 0) {
                        break;
                    }
                }

                Assert.assertEquals(1, tenant.getLocalWatchCount());
                Assert.assertTrue(tenant.runsWatchLocally("test_watch"));

                Thread.sleep(500);

                List<String> ackedActions = tenant.ack("test_watch", new User("horst"));
                Assert.assertEquals(Arrays.asList("testsink"), ackedActions);

                ackedTime1 = tenant.getWatchStateManager().getWatchState("test_watch").getActionState("testsink").getAcked();

                Assert.assertNotNull(ackedTime1);

                Thread.sleep(500);
            }

            Thread.sleep(1000);

            try (SignalsTenant tenant = new SignalsTenant("failover_test", client, clusterService, nodeEnvironment, scriptService, xContentRegistry,
                    internalAuthTokenProvider, new SignalsSettings(settings), null, diagnosticContext)) {
                tenant.init();

                for (int i = 0; i < 20; i++) {
                    Thread.sleep(100);

                    if (tenant.getLocalWatchCount() != 0) {
                        break;
                    }
                }

                Assert.assertEquals(1, tenant.getLocalWatchCount());
                Assert.assertTrue(tenant.runsWatchLocally("test_watch"));

                Ack ackedTime2 = tenant.getWatchStateManager().getWatchState("test_watch").getActionState("testsink").getAcked();

                Assert.assertEquals(ackedTime1, ackedTime2);
            }
        }

    }

    @Test
    public void failoverWhileRunningTest() throws Exception {

        try (Client client = cluster.getInternalNodeClient()) {

            Settings settings = Settings.builder().build();

            try (SignalsTenant tenant = new SignalsTenant("failover_while_running_test", client, clusterService, nodeEnvironment, scriptService,
                    xContentRegistry, internalAuthTokenProvider, new SignalsSettings(settings), null, diagnosticContext)) {
                tenant.init();

                Watch watch = new WatchBuilder("test_watch").atInterval("100ms").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                        .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().act(new SleepAction(Duration.ofSeconds(4))).name("sleep").and()
                        .index("failover_while_running_testsink").name("testsink").build();

                tenant.addWatch(watch, UHURA);

                for (int i = 0; i < 20; i++) {
                    Thread.sleep(100);

                    if (tenant.getLocalWatchCount() != 0) {
                        break;
                    }
                }

                Assert.assertEquals(1, tenant.getLocalWatchCount());
                Assert.assertTrue(tenant.runsWatchLocally("test_watch"));

                Thread.sleep(500);

                tenant.shutdownHard();
            }

            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

            Thread.sleep(1000);

            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

            try (SignalsTenant tenant = new SignalsTenant("failover_while_running_test", client, clusterService, nodeEnvironment, scriptService,
                    xContentRegistry, internalAuthTokenProvider, new SignalsSettings(settings), null, diagnosticContext)) {
                tenant.init();

                for (int i = 0; i < 20; i++) {
                    Thread.sleep(100);

                    if (tenant.getLocalWatchCount() != 0) {
                        break;
                    }
                }

                Assert.assertEquals(1, tenant.getLocalWatchCount());
                Assert.assertTrue(tenant.runsWatchLocally("test_watch"));
            }
        }

    }

    static {
        ActionHandler.factoryRegistry.add(new SleepAction.Factory());
    }

    static class SleepAction extends ActionHandler {

        private Duration duration;

        SleepAction(Duration duration) {
            this.duration = duration;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("duration", DurationFormat.INSTANCE.format(duration));
            return builder;
        }

        @Override
        public ActionExecutionResult execute(WatchExecutionContext ctx) throws ActionExecutionException {
            try {
                Thread.sleep(duration.toMillis());
            } catch (InterruptedException e) {
            }

            return new ActionExecutionResult("zzz");
        }

        @Override
        public String getType() {
            return "sleep";
        }

        public static class Factory extends ActionHandler.Factory<SleepAction> {
            public Factory() {
                super("sleep");
            }

            @Override
            protected SleepAction create(WatchInitializationService watchInitService, ValidatingJsonNode vJsonNode, ValidationErrors validationErrors)
                    throws ConfigValidationException {

                return new SleepAction(vJsonNode.duration("duration"));
            }
        }
    }

}
