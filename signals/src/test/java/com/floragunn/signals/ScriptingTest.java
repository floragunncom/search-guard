package com.floragunn.signals;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import com.floragunn.signals.proxy.service.HttpProxyHostRegistry;
import com.floragunn.signals.truststore.service.TrustManagerRegistry;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.signals.execution.ExecutionEnvironment;
import com.floragunn.signals.execution.SimulationMode;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.execution.WatchExecutionContextData;
import com.floragunn.signals.execution.WatchExecutionContextData.TriggerInfo;
import com.floragunn.signals.execution.WatchExecutionContextData.WatchInfo;
import com.floragunn.signals.script.types.SignalsObjectFunctionScript;
import com.floragunn.signals.support.NestedValueMap;
import com.floragunn.signals.watch.action.invokers.ActionInvocationType;
import com.floragunn.signals.watch.init.WatchInitializationService;

import net.jcip.annotations.NotThreadSafe;
import org.mockito.Mockito;

import static com.floragunn.signals.watch.common.ValidationLevel.STRICT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@NotThreadSafe
public class ScriptingTest {
    private static NamedXContentRegistry xContentRegistry;
    private static ScriptService scriptService;
    private static WatchInitializationService watchInitService;
    private static SignalsModule signalsModule;

    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().singleNode().sslEnabled().resources("sg_config/signals")
            .nodeSettings("signals.enabled", true, "signals.index_names.log", "signals_main_log", "searchguard.enterprise_modules_enabled", false)
            .nodeSettings("script.max_compilations_rate", "1/1m")
            .enableModule(SignalsModule.class).waitForComponents("signals").embedded().build();

    @BeforeClass
    public static void setupDependencies() {
        xContentRegistry = cluster.getInjectable(NamedXContentRegistry.class);
        scriptService = cluster.getInjectable(ScriptService.class);
        watchInitService = new WatchInitializationService(null, scriptService, Mockito.mock(TrustManagerRegistry.class),
                Mockito.mock(HttpProxyHostRegistry.class), null, STRICT);
        signalsModule = cluster.getInjectable(SignalsModule.class);
    }

    @Test
    public void signalsScriptContextsShouldHaveUnlimitedCompilationRate() {
        List<ScriptContext<?>> signalsScriptContexts = signalsModule.getContexts();
        int noOfScriptCompilationsPerContext = 1000;
        //depends on the order in which the tests are run
        long minExpectedNoOfCompilations = (long) signalsScriptContexts.size() * noOfScriptCompilationsPerContext;

        for (ScriptContext<?> scriptContext : signalsScriptContexts) {
            for (int i = 0; i < noOfScriptCompilationsPerContext; i++) {
                scriptService.compile(new Script(ScriptType.INLINE, "painless", "params." + scriptContext.name + i, Collections.emptyMap()), scriptContext);
            }
        }
        assertThat(scriptService.cacheStats().getGeneralStats().getCompilations(), greaterThanOrEqualTo(minExpectedNoOfCompilations));
    }

    @Test
    public void testPropertyAccessForTriggeredTime() {
        ValidationErrors validationErrors = new ValidationErrors();

        SignalsObjectFunctionScript.Factory factory = watchInitService.compile("test", "trigger.triggered_time", "painless",
                SignalsObjectFunctionScript.CONTEXT, validationErrors);

        Assert.assertFalse(validationErrors.toString(), validationErrors.hasErrors());

        WatchExecutionContextData watchExecutionContextData = new WatchExecutionContextData(new NestedValueMap(),
                new WatchInfo("test_id", "test_tenant"), new TriggerInfo(new Date(1234), new Date(4567), new Date(), new Date()), null);

        WatchExecutionContext ctx = new WatchExecutionContext(null, scriptService, xContentRegistry, null, ExecutionEnvironment.TEST,
                ActionInvocationType.ALERT, watchExecutionContextData, null, SimulationMode.SIMULATE_ACTIONS, null, null, null, null,
                Mockito.mock(TrustManagerRegistry.class));

        SignalsObjectFunctionScript script = factory.newInstance(new HashMap<String, Object>(), ctx);

        Object result = script.execute();

        Assert.assertEquals(watchExecutionContextData.getTriggerInfo().getTriggeredTime(), result);
    }

    @Test
    public void testPropertyAccessForWatchId() {
        ValidationErrors validationErrors = new ValidationErrors();

        SignalsObjectFunctionScript.Factory factory = watchInitService.compile("test", "watch.id", "painless", SignalsObjectFunctionScript.CONTEXT,
                validationErrors);

        Assert.assertFalse(validationErrors.toString(), validationErrors.hasErrors());

        WatchExecutionContextData watchExecutionContextData = new WatchExecutionContextData(new NestedValueMap(),
                new WatchInfo("test_id", "test_tenant"), new TriggerInfo(new Date(1234), new Date(4567), new Date(), new Date()), null);

        WatchExecutionContext ctx = new WatchExecutionContext(null, scriptService, xContentRegistry, null, ExecutionEnvironment.TEST,
                ActionInvocationType.ALERT, watchExecutionContextData, null, SimulationMode.SIMULATE_ACTIONS, null, null, null, null,
                Mockito.mock(TrustManagerRegistry.class));

        SignalsObjectFunctionScript script = factory.newInstance(new HashMap<String, Object>(), ctx);

        Object result = script.execute();

        Assert.assertEquals(watchExecutionContextData.getWatch().getId(), result);
    }
}
