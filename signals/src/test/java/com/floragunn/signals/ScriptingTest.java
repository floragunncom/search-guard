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
package com.floragunn.signals;

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
import java.util.Date;
import java.util.HashMap;
import net.jcip.annotations.NotThreadSafe;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

@NotThreadSafe
public class ScriptingTest {
    private static NamedXContentRegistry xContentRegistry;
    private static ScriptService scriptService;
    private static WatchInitializationService watchInitService;

    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().resources("sg_config/signals")
            .nodeSettings("signals.enabled", true, "signals.index_names.log", "signals_main_log", "searchguard.enterprise_modules_enabled", false)
            .enableModule(SignalsModule.class).build();

    @BeforeClass
    public static void setupDependencies() {
        xContentRegistry = cluster.getInjectable(NamedXContentRegistry.class);
        scriptService = cluster.getInjectable(ScriptService.class);
        watchInitService = new WatchInitializationService(null, scriptService);
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
                ActionInvocationType.ALERT, watchExecutionContextData, null, SimulationMode.SIMULATE_ACTIONS, null, null, null, null);

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
                ActionInvocationType.ALERT, watchExecutionContextData, null, SimulationMode.SIMULATE_ACTIONS, null, null, null, null);

        SignalsObjectFunctionScript script = factory.newInstance(new HashMap<String, Object>(), ctx);

        Object result = script.execute();

        Assert.assertEquals(watchExecutionContextData.getWatch().getId(), result);
    }
}
