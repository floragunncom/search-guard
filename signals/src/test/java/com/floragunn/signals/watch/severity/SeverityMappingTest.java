package com.floragunn.signals.watch.severity;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Map;

import com.floragunn.signals.proxy.service.HttpProxyHostRegistry;
import com.floragunn.signals.truststore.service.TrustManagerRegistry;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.signals.SignalsModule;
import com.floragunn.signals.execution.ExecutionEnvironment;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.execution.WatchExecutionContextData;
import com.floragunn.signals.support.NestedValueMap;
import com.floragunn.signals.watch.action.invokers.ActionInvocationType;
import com.floragunn.signals.watch.init.WatchInitializationService;
import com.floragunn.signals.watch.severity.SeverityMapping.EvaluationResult;
import org.mockito.Mockito;

import static com.floragunn.signals.watch.common.ValidationLevel.STRICT;

public class SeverityMappingTest {
    private static NamedXContentRegistry xContentRegistry;
    private static ScriptService scriptService;

    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().singleNode().sslEnabled()
            .nodeSettings("signals.enabled", true, "signals.enterprise.enabled", false).resources("sg_config/signals")
            .enableModule(SignalsModule.class).embedded().build();

    @BeforeClass
    public static void setupDependencies() {
        xContentRegistry = cluster.getInjectable(NamedXContentRegistry.class);
        scriptService = cluster.getInjectable(ScriptService.class);
    }

    @Test
    public void basicTest() throws Exception {
        WatchInitializationService watchInitService = new WatchInitializationService(null, null,
            Mockito.mock(TrustManagerRegistry.class), Mockito.mock(HttpProxyHostRegistry.class), null, STRICT);

        DocNode config = DocNode.of("value", "x", "mapping",
                Arrays.asList(DocNode.of("threshold", 1, "level", "info"), DocNode.of("threshold", 2, "level", "error")));

        SeverityMapping severityMapping = SeverityMapping.create(watchInitService, config);

        Assert.assertEquals(severityMapping.getMapping().get(0).getThreshold(), new BigDecimal(1));
        Assert.assertEquals(severityMapping.getMapping().get(1).getThreshold(), new BigDecimal(2));
        Assert.assertEquals(severityMapping.getMapping().get(0).getLevel(), SeverityLevel.INFO);
        Assert.assertEquals(severityMapping.getMapping().get(1).getLevel(), SeverityLevel.ERROR);
    }

    @Test
    public void reorderTest() throws Exception {
        WatchInitializationService watchInitService = new WatchInitializationService(null, null,
            Mockito.mock(TrustManagerRegistry.class), Mockito.mock(HttpProxyHostRegistry.class), null, STRICT);

        DocNode config = DocNode.of("value", "x", "mapping",
                Arrays.asList(DocNode.of("threshold", 2, "level", "error"), DocNode.of("threshold", 1, "level", "info")));

        SeverityMapping severityMapping = SeverityMapping.create(watchInitService, config);

        Assert.assertEquals(severityMapping.getMapping().get(0).getThreshold(), new BigDecimal(1));
        Assert.assertEquals(severityMapping.getMapping().get(1).getThreshold(), new BigDecimal(2));
        Assert.assertEquals(severityMapping.getMapping().get(0).getLevel(), SeverityLevel.INFO);
        Assert.assertEquals(severityMapping.getMapping().get(1).getLevel(), SeverityLevel.ERROR);
    }

    @Test
    public void descendingOrderTest() throws Exception {
        WatchInitializationService watchInitService = new WatchInitializationService(null, null,
            Mockito.mock(TrustManagerRegistry.class), Mockito.mock(HttpProxyHostRegistry.class), null, STRICT);

        DocNode config = DocNode.of("value", "x", "order", "descending", "mapping",
                Arrays.asList(DocNode.of("threshold", 2, "level", "info"), DocNode.of("threshold", 1, "level", "error")));

        SeverityMapping severityMapping = SeverityMapping.create(watchInitService, config);

        Assert.assertEquals(severityMapping.getMapping().get(0).getThreshold(), new BigDecimal(2));
        Assert.assertEquals(severityMapping.getMapping().get(1).getThreshold(), new BigDecimal(1));
        Assert.assertEquals(severityMapping.getMapping().get(0).getLevel(), SeverityLevel.INFO);
        Assert.assertEquals(severityMapping.getMapping().get(1).getLevel(), SeverityLevel.ERROR);
    }

    @Test
    public void descendingReOrderTest() throws Exception {
        WatchInitializationService watchInitService = new WatchInitializationService(null, null,
            Mockito.mock(TrustManagerRegistry.class), Mockito.mock(HttpProxyHostRegistry.class), null, STRICT);

        DocNode config = DocNode.of("value", "x", "order", "descending", "mapping",
                Arrays.asList(DocNode.of("threshold", 1, "level", "error"), DocNode.of("threshold", 2, "level", "info")));

        SeverityMapping severityMapping = SeverityMapping.create(watchInitService, config);

        Assert.assertEquals(severityMapping.getMapping().get(0).getThreshold(), new BigDecimal(2));
        Assert.assertEquals(severityMapping.getMapping().get(1).getThreshold(), new BigDecimal(1));
        Assert.assertEquals(severityMapping.getMapping().get(0).getLevel(), SeverityLevel.INFO);
        Assert.assertEquals(severityMapping.getMapping().get(1).getLevel(), SeverityLevel.ERROR);
    }

    @Test
    public void duplicateTest() throws Exception {
        WatchInitializationService watchInitService = new WatchInitializationService(null, null,
            Mockito.mock(TrustManagerRegistry.class), Mockito.mock(HttpProxyHostRegistry.class), null, STRICT);

        DocNode config = DocNode.of("value", "x", "mapping",
                Arrays.asList(DocNode.of("threshold", 1, "level", "info"), DocNode.of("threshold", 1, "level", "error")));

        try {
            SeverityMapping severityMapping = SeverityMapping.create(watchInitService, config);
            Assert.fail(severityMapping.toString());
        } catch (ConfigValidationException e) {
            Assert.assertTrue(e.toString(), e.getMessage().contains("Contains duplicate thresholds: 1"));
        }
    }

    @Test
    public void findValueTest() throws Exception {
        WatchInitializationService watchInitService = new WatchInitializationService(null, null,
            Mockito.mock(TrustManagerRegistry.class), Mockito.mock(HttpProxyHostRegistry.class), null, STRICT);

        DocNode config = DocNode.of("value", "x", "mapping",
                Arrays.asList(DocNode.of("threshold", 1, "level", "info"), DocNode.of("threshold", 2, "level", "error")));

        SeverityMapping severityMapping = SeverityMapping.create(watchInitService, config);

        SeverityMapping.Element element = severityMapping.findMatchingMappingElement(new BigDecimal("0.9"));
        Assert.assertNull(element);

        element = severityMapping.findMatchingMappingElement(new BigDecimal("1.0"));
        Assert.assertEquals(new SeverityMapping.Element(new BigDecimal("1.0"), SeverityLevel.INFO), element);

        element = severityMapping.findMatchingMappingElement(new BigDecimal("5.0"));
        Assert.assertEquals(new SeverityMapping.Element(new BigDecimal("2.0"), SeverityLevel.ERROR), element);
    }

    @Test
    public void findValueWithBigNumbersTest() throws Exception {
        WatchInitializationService watchInitService = new WatchInitializationService(null, null,
            Mockito.mock(TrustManagerRegistry.class), Mockito.mock(HttpProxyHostRegistry.class), null, STRICT);

        String configJson = "{\"value\": \"data.x\", \"mapping\": [{\"threshold\": 123456789999, \"level\": \"info\"}, {\"threshold\": 223456789999, \"level\": \"error\"}]}";

        DocNode config = DocNode.parse(Format.JSON).from(configJson);

        SeverityMapping severityMapping = SeverityMapping.create(watchInitService, config);

        SeverityMapping.Element element = severityMapping.findMatchingMappingElement(new BigDecimal("2"));
        Assert.assertNull(element);

        element = severityMapping.findMatchingMappingElement(new BigDecimal("123456799999"));
        Assert.assertEquals(new SeverityMapping.Element(new BigDecimal("123456789999"), SeverityLevel.INFO), element);

        element = severityMapping.findMatchingMappingElement(new BigDecimal("223457789999"));
        Assert.assertEquals(new SeverityMapping.Element(new BigDecimal("223456789999"), SeverityLevel.ERROR), element);
    }

    @Test
    public void descendingFindValueTest() throws Exception {
        WatchInitializationService watchInitService = new WatchInitializationService(null, null,
            Mockito.mock(TrustManagerRegistry.class), Mockito.mock(HttpProxyHostRegistry.class), null, STRICT);

        DocNode config = DocNode.of("value", "x", "order", "descending", "mapping",
                Arrays.asList(DocNode.of("threshold", 1, "level", "error"), DocNode.of("threshold", 2, "level", "info")));

        SeverityMapping severityMapping = SeverityMapping.create(watchInitService, config);

        SeverityMapping.Element element = severityMapping.findMatchingMappingElement(new BigDecimal("2.9"));
        Assert.assertNull(element);

        element = severityMapping.findMatchingMappingElement(new BigDecimal("2.0"));
        Assert.assertEquals(new SeverityMapping.Element(new BigDecimal("2.0"), SeverityLevel.INFO), element);

        element = severityMapping.findMatchingMappingElement(new BigDecimal("-5.0"));
        Assert.assertEquals(new SeverityMapping.Element(new BigDecimal("1.0"), SeverityLevel.ERROR), element);
    }

    @Test
    public void evaluationResultTest() throws Exception {
        WatchInitializationService watchInitService = new WatchInitializationService(null, scriptService,
            Mockito.mock(TrustManagerRegistry.class), Mockito.mock(HttpProxyHostRegistry.class), null, STRICT);

        String configJson = "{\n" + "    \"mapping\": [\n" + "      {\n" + "        \"level\": \"info\",\n" + "        \"threshold\": 100\n"
                + "      },\n" + "      {\n" + "        \"level\": \"warning\",\n" + "        \"threshold\": 200\n" + "      },\n" + "      {\n"
                + "        \"level\": \"error\",\n" + "        \"threshold\": 300\n" + "      },\n" + "      {\n"
                + "        \"level\": \"critical\",\n" + "        \"threshold\": 400\n" + "      }\n" + "    ],\n" + "    \"value\": \"data.a\",\n"
                + "    \"order\": \"ascending\"\n" + "  }";

        DocNode config = DocNode.parse(Format.JSON).from(configJson);

        SeverityMapping severityMapping = SeverityMapping.create(watchInitService, config);

        NestedValueMap runtimeData = new NestedValueMap();
        runtimeData.put("a", 10);

        WatchExecutionContext ctx = new WatchExecutionContext(null, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData), Mockito.mock(TrustManagerRegistry.class));

        EvaluationResult evaluationResult = severityMapping.execute(ctx);
        Map<String, Object> evaluationResultMap = evaluationResult.toMap();

        Assert.assertNull(evaluationResult.getMappingElement());
        Assert.assertEquals(SeverityLevel.NONE, evaluationResult.getLevel());
        Assert.assertEquals(evaluationResult.getLevel().toMap(), evaluationResultMap.get("level"));

        runtimeData.put("a", 150);

        ctx = new WatchExecutionContext(null, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED, ActionInvocationType.ALERT,
                new WatchExecutionContextData(runtimeData), Mockito.mock(TrustManagerRegistry.class));

        evaluationResult = severityMapping.execute(ctx);
        evaluationResultMap = evaluationResult.toMap();

        Assert.assertEquals(evaluationResult.getMappingElement().toMap(), evaluationResultMap.get("mapping_element"));
        Assert.assertEquals(SeverityLevel.INFO, evaluationResult.getLevel());
        Assert.assertEquals(evaluationResult.getLevel().toMap(), evaluationResultMap.get("level"));

    }

}
