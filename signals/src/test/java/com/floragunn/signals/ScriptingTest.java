package com.floragunn.signals;

import java.util.Date;
import java.util.HashMap;

import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.script.ScriptService;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchsupport.jobs.config.validation.ValidationErrors;
import com.floragunn.signals.execution.ExecutionEnvironment;
import com.floragunn.signals.execution.SimulationMode;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.execution.WatchExecutionContextData;
import com.floragunn.signals.execution.WatchExecutionContextData.TriggerInfo;
import com.floragunn.signals.script.types.SignalsObjectFunctionScript;
import com.floragunn.signals.support.NestedValueMap;
import com.floragunn.signals.watch.action.invokers.ActionInvocationType;
import com.floragunn.signals.watch.init.WatchInitializationService;

public class ScriptingTest {

    private static NamedXContentRegistry xContentRegistry;
    private static ScriptService scriptService;
    private static WatchInitializationService watchInitService;

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().resources("sg_config/signals")
            .nodeSettings("signals.enabled", true, "signals.index_names.log", "signals_main_log", "searchguard.enterprise_modules_enabled", false)
            .build();

    @BeforeClass
    public static void setupDependencies() {
        xContentRegistry = cluster.getInjectable(NamedXContentRegistry.class);
        scriptService = cluster.getInjectable(ScriptService.class);
        watchInitService = new WatchInitializationService(null, scriptService);
    }

    @Test
    public void testPropertyAccess() {
        ValidationErrors validationErrors = new ValidationErrors();

        SignalsObjectFunctionScript.Factory factory = watchInitService.compile("test", "trigger.triggered_time", "painless",
                SignalsObjectFunctionScript.CONTEXT, validationErrors);

        Assert.assertFalse(validationErrors.toString(), validationErrors.hasErrors());

        WatchExecutionContextData watchExecutionContextData = new WatchExecutionContextData(new NestedValueMap(),
                new TriggerInfo(new Date(1234), new Date(4567), new Date(), new Date()), null);

        WatchExecutionContext ctx = new WatchExecutionContext(null, scriptService, xContentRegistry, null, ExecutionEnvironment.TEST,
                ActionInvocationType.ALERT, watchExecutionContextData, null, SimulationMode.SIMULATE_ACTIONS, null);

        SignalsObjectFunctionScript script = factory.newInstance(new HashMap<String, Object>(), ctx);

        Object result = script.execute();

        Assert.assertEquals(watchExecutionContextData.getTriggerInfo().getTriggeredTime(), result);
    }

}
