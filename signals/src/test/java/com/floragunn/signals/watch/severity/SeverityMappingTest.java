package com.floragunn.signals.watch.severity;

import java.math.BigDecimal;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.signals.support.JsonBuilder;
import com.floragunn.signals.watch.init.WatchInitializationService;

public class SeverityMappingTest {
    @Test
    public void basicTest() throws Exception {
        WatchInitializationService watchInitService = new WatchInitializationService(null, null);

        JsonNode config = new JsonBuilder.Object().attr("value", "x")
                .attr("mapping", new JsonBuilder.Array(new JsonBuilder.Object().attr("threshold", 1).attr("level", "info"),
                        new JsonBuilder.Object().attr("threshold", 2).attr("level", "error")))
                .getNode();

        SeverityMapping severityMapping = SeverityMapping.create(watchInitService, config);

        Assert.assertEquals(severityMapping.getMapping().get(0).getThreshold(), new BigDecimal(1));
        Assert.assertEquals(severityMapping.getMapping().get(1).getThreshold(), new BigDecimal(2));
        Assert.assertEquals(severityMapping.getMapping().get(0).getLevel(), SeverityLevel.INFO);
        Assert.assertEquals(severityMapping.getMapping().get(1).getLevel(), SeverityLevel.ERROR);
    }

    @Test
    public void reorderTest() throws Exception {
        WatchInitializationService watchInitService = new WatchInitializationService(null, null);

        JsonNode config = new JsonBuilder.Object().attr("value", "x")
                .attr("mapping", new JsonBuilder.Array(new JsonBuilder.Object().attr("threshold", 2).attr("level", "error"),
                        new JsonBuilder.Object().attr("threshold", 1).attr("level", "info")))
                .getNode();

        SeverityMapping severityMapping = SeverityMapping.create(watchInitService, config);

        Assert.assertEquals(severityMapping.getMapping().get(0).getThreshold(), new BigDecimal(1));
        Assert.assertEquals(severityMapping.getMapping().get(1).getThreshold(), new BigDecimal(2));
        Assert.assertEquals(severityMapping.getMapping().get(0).getLevel(), SeverityLevel.INFO);
        Assert.assertEquals(severityMapping.getMapping().get(1).getLevel(), SeverityLevel.ERROR);
    }

    @Test
    public void descendingOrderTest() throws Exception {
        WatchInitializationService watchInitService = new WatchInitializationService(null, null);

        JsonNode config = new JsonBuilder.Object().attr("value", "x").attr("order", "descending")
                .attr("mapping", new JsonBuilder.Array(new JsonBuilder.Object().attr("threshold", 2).attr("level", "info"),
                        new JsonBuilder.Object().attr("threshold", 1).attr("level", "error")))
                .getNode();

        SeverityMapping severityMapping = SeverityMapping.create(watchInitService, config);

        Assert.assertEquals(severityMapping.getMapping().get(0).getThreshold(), new BigDecimal(2));
        Assert.assertEquals(severityMapping.getMapping().get(1).getThreshold(), new BigDecimal(1));
        Assert.assertEquals(severityMapping.getMapping().get(0).getLevel(), SeverityLevel.INFO);
        Assert.assertEquals(severityMapping.getMapping().get(1).getLevel(), SeverityLevel.ERROR);
    }

    @Test
    public void descendingReOrderTest() throws Exception {
        WatchInitializationService watchInitService = new WatchInitializationService(null, null);

        JsonNode config = new JsonBuilder.Object().attr("value", "x").attr("order", "descending")
                .attr("mapping", new JsonBuilder.Array(new JsonBuilder.Object().attr("threshold", 1).attr("level", "error"),
                        new JsonBuilder.Object().attr("threshold", 2).attr("level", "info")))
                .getNode();

        SeverityMapping severityMapping = SeverityMapping.create(watchInitService, config);

        Assert.assertEquals(severityMapping.getMapping().get(0).getThreshold(), new BigDecimal(2));
        Assert.assertEquals(severityMapping.getMapping().get(1).getThreshold(), new BigDecimal(1));
        Assert.assertEquals(severityMapping.getMapping().get(0).getLevel(), SeverityLevel.INFO);
        Assert.assertEquals(severityMapping.getMapping().get(1).getLevel(), SeverityLevel.ERROR);
    }

    @Test
    public void duplicateTest() throws Exception {
        WatchInitializationService watchInitService = new WatchInitializationService(null, null);

        JsonNode config = new JsonBuilder.Object().attr("value", "x")
                .attr("mapping", new JsonBuilder.Array(new JsonBuilder.Object().attr("threshold", 1).attr("level", "info"),
                        new JsonBuilder.Object().attr("threshold", 1).attr("level", "error")))
                .getNode();

        try {
            SeverityMapping severityMapping = SeverityMapping.create(watchInitService, config);
            Assert.fail(severityMapping.toString());
        } catch (ConfigValidationException e) {
            Assert.assertTrue(e.toString(), e.getMessage().contains("Contains duplicate thresholds: 1"));
        }
    }

    @Test
    public void findValueTest() throws Exception {
        WatchInitializationService watchInitService = new WatchInitializationService(null, null);

        JsonNode config = new JsonBuilder.Object().attr("value", "x")
                .attr("mapping", new JsonBuilder.Array(new JsonBuilder.Object().attr("threshold", 1).attr("level", "info"),
                        new JsonBuilder.Object().attr("threshold", 2).attr("level", "error")))
                .getNode();

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
        WatchInitializationService watchInitService = new WatchInitializationService(null, null);
        
        String configJson = "{\"value\": \"data.x\", \"mapping\": [{\"threshold\": 123456789999, \"level\": \"info\"}, {\"threshold\": 223456789999, \"level\": \"error\"}]}";

        JsonNode config = DefaultObjectMapper.readTree(configJson);
        
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
        WatchInitializationService watchInitService = new WatchInitializationService(null, null);

        JsonNode config = new JsonBuilder.Object().attr("value", "x").attr("order", "descending")
                .attr("mapping", new JsonBuilder.Array(new JsonBuilder.Object().attr("threshold", 1).attr("level", "error"),
                        new JsonBuilder.Object().attr("threshold", 2).attr("level", "info")))
                .getNode();

        SeverityMapping severityMapping = SeverityMapping.create(watchInitService, config);

        SeverityMapping.Element element = severityMapping.findMatchingMappingElement(new BigDecimal("2.9"));
        Assert.assertNull(element);

        element = severityMapping.findMatchingMappingElement(new BigDecimal("2.0"));
        Assert.assertEquals(new SeverityMapping.Element(new BigDecimal("2.0"), SeverityLevel.INFO), element);

        element = severityMapping.findMatchingMappingElement(new BigDecimal("-5.0"));
        Assert.assertEquals(new SeverityMapping.Element(new BigDecimal("1.0"), SeverityLevel.ERROR), element);
    }

}
