package com.floragunn.signals.watch.severity;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.config.validation.ValidationError;
import com.floragunn.searchsupport.config.validation.ValidationErrors;
import com.floragunn.searchsupport.xcontent.ObjectTreeXContent;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.execution.WatchOperationExecutionException;
import com.floragunn.signals.script.SignalsScript;
import com.floragunn.signals.watch.init.WatchInitializationService;

public class SeverityMapping implements ToXContentObject {

    private String value;
    private String lang;
    private List<Element> mapping;
    private SeverityValueScript.Factory scriptFactory;
    private Order order;

    public SeverityMapping(String value, String lang, Order order, List<Element> mapping) {
        this.value = value;
        this.lang = lang;
        this.order = order != null ? order : Order.ASCENDING;
        this.mapping = mapping;
    }

    public EvaluationResult execute(WatchExecutionContext ctx) throws WatchOperationExecutionException {
        try {
            SeverityValueScript conditionScript = scriptFactory.newInstance(null, ctx);
            Number currentValue = conditionScript.execute();

            if (currentValue == null || isNaN(currentValue)) {
                // TODO fail? warning?
                return new EvaluationResult(SeverityLevel.NONE, null, currentValue);
            }

            Element element = findMatchingMappingElement(new BigDecimal(currentValue.toString()));

            if (element != null) {
                return new EvaluationResult(element.getLevel(), element, currentValue);
            } else {
                return new EvaluationResult(SeverityLevel.NONE, null, currentValue);
            }

        } catch (ScriptException e) {
            throw new WatchOperationExecutionException(e);
        }
    }

    public BigDecimal getFirstThreshold() {
        if (mapping.size() > 0) {
            return mapping.get(0).getThreshold();
        } else {
            return null;
        }
    }

    public Element findMatchingMappingElement(BigDecimal value) {
        Element result = null;

        for (Element element : this.mapping) {
            if (element.threshold.compareTo(value) * order.getFactor() > 0) {
                return result;
            }

            result = element;
        }

        return result;
    }

    private boolean isNaN(Number number) {
        if (number == null) {
            return true;
        }

        if ((number instanceof Double) && ((Double) number).isNaN()) {
            return true;
        }

        if ((number instanceof Float) && ((Float) number).isNaN()) {
            return true;
        }

        return false;
    }

    public void compileScripts(WatchInitializationService watchInitService) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();

        if (value != null) {

            Script script = new Script(ScriptType.INLINE, lang != null ? lang : "painless", value, new HashMap<>());

            this.scriptFactory = watchInitService.compile("value", script, SeverityValueScript.CONTEXT, validationErrors);
        }

        validationErrors.throwExceptionForPresentErrors();
    }

    public Set<SeverityLevel> getDefinedLevels() {
        HashSet<SeverityLevel> result = new HashSet<>();

        for (Element element : mapping) {
            result.add(element.level);
        }

        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("value", value);

        if (lang != null) {
            builder.field("lang", lang);
        }

        if (order != null) {
            builder.field("order", order);
        }

        builder.field("mapping", mapping);

        builder.endObject();

        return builder;
    }

    public static class Element implements ToXContent {

        private final BigDecimal threshold;
        private final SeverityLevel level;

        public Element(BigDecimal threshold, SeverityLevel level) {
            this.threshold = threshold;
            this.level = level;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("threshold", threshold);
            builder.field("level", level.getId());
            builder.endObject();
            return builder;
        }

        public Map<String, Object> toMap() {
            return ObjectTreeXContent.toMap(this);
        }

        static List<Element> createList(ArrayNode arrayNode, Order order) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();

            if (arrayNode == null) {
                return null;
            }

            ArrayList<Element> result = new ArrayList<>(arrayNode.size());

            for (JsonNode jsonNode : arrayNode) {
                try {
                    result.add(create(jsonNode));
                } catch (ConfigValidationException e) {
                    validationErrors.add(null, e);
                }
            }

            validationErrors.throwExceptionForPresentErrors();

            result.sort((e1, e2) -> e1.threshold.compareTo(e2.threshold) * order.getFactor());

            checkForDuplicateValuesAndOrdering(result, order);

            return result;
        }

        static void checkForDuplicateValuesAndOrdering(ArrayList<Element> sortedElements, Order order) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();

            HashSet<BigDecimal> duplicateThresholds = new HashSet<>();

            for (int i = 0; i < sortedElements.size() - 1; i++) {
                if (sortedElements.get(i).threshold.compareTo(sortedElements.get(i + 1).threshold) == 0) {
                    duplicateThresholds.add(sortedElements.get(i).threshold);
                    i++;
                }
            }

            if (duplicateThresholds.size() > 0) {
                validationErrors.add(new ValidationError("threshold", "Contains duplicate thresholds: " + Strings.join(duplicateThresholds, ',')));
            }

            HashSet<SeverityLevel> seenLevels = new HashSet<>();
            List<SeverityLevel> duplicateLevels = new ArrayList<>();

            for (Element e : sortedElements) {
                if (seenLevels.contains(e.level)) {
                    duplicateLevels.add(e.level);
                } else {
                    seenLevels.add(e.level);
                }
            }

            if (duplicateLevels.size() > 0) {
                validationErrors.add(new ValidationError("level", "Contains duplicate levels: " + Strings.join(duplicateLevels, ',')));

            }

            validationErrors.throwExceptionForPresentErrors();

            for (int i = 0; i < sortedElements.size() - 1; i++) {
                if (SeverityLevel.compare(sortedElements.get(i).level, sortedElements.get(i + 1).level) > 0) {
                    String relation = order == Order.ASCENDING ? " > " : " < ";

                    validationErrors.add(new ValidationError(null,
                            "The ordering of the thresholds is not consistent to the ordering of the levels: " + sortedElements.get(i).threshold
                                    + relation + sortedElements.get(i + 1).threshold + " but " + sortedElements.get(i).level + " > "
                                    + sortedElements.get(i + 1).level));

                    validationErrors.throwExceptionForPresentErrors();

                    return;
                }
            }
        }

        static Element create(JsonNode jsonNode) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode, validationErrors);

            BigDecimal threshold = vJsonNode.requiredBigDecimal("threshold");
            SeverityLevel level = vJsonNode.requiredCaseInsensitiveEnum("level", SeverityLevel.class);

            vJsonNode.validateUnusedAttributes();
            validationErrors.throwExceptionForPresentErrors();

            return new Element(threshold, level);
        }

        public BigDecimal getThreshold() {
            return threshold;
        }

        public SeverityLevel getLevel() {
            return level;
        }

        @Override
        public String toString() {
            return "Element [threshold=" + threshold + ", level=" + level + "]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((level == null) ? 0 : level.hashCode());
            result = prime * result + ((threshold == null) ? 0 : threshold.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Element other = (Element) obj;
            if (level != other.level)
                return false;
            if (threshold == null) {
                if (other.threshold != null)
                    return false;
            } else if (threshold.compareTo(other.threshold) != 0)
                return false;
            return true;
        }

    }

    public static enum Order {

        ASCENDING(1), DESCENDING(-1);

        private final int factor;

        Order(int factor) {
            this.factor = factor;
        }

        public String toString() {
            return name().toLowerCase();
        }

        public int getFactor() {
            return factor;
        }
    }

    public static SeverityMapping create(WatchInitializationService watchInitService, JsonNode jsonNode) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode, validationErrors);

        String value = vJsonNode.requiredString("value");
        String lang = vJsonNode.string("lang");
        ArrayNode mappingArray = vJsonNode.requiredArray("mapping");
        Order order = vJsonNode.caseInsensitiveEnum("order", Order.class, Order.ASCENDING);

        List<Element> mapping = null;

        try {
            mapping = Element.createList(mappingArray, order);
        } catch (ConfigValidationException e) {
            validationErrors.add("mapping", e);
        }

        validationErrors.throwExceptionForPresentErrors();

        SeverityMapping result = new SeverityMapping(value, lang, order, mapping);

        result.compileScripts(watchInitService);

        validationErrors.throwExceptionForPresentErrors();

        return result;
    }

    public static abstract class SeverityValueScript extends SignalsScript {

        public static final String[] PARAMETERS = {};

        public SeverityValueScript(Map<String, Object> params, WatchExecutionContext watchRuntimeContext) {
            super(params, watchRuntimeContext);
        }

        public abstract Number execute();

        public static interface Factory {
            SeverityValueScript newInstance(Map<String, Object> params, WatchExecutionContext watcherContext);
        }

        public static ScriptContext<Factory> CONTEXT = new ScriptContext<>("signals_severity_value", Factory.class);

    }

    public static class EvaluationResult implements ToXContentObject {
        private final SeverityLevel level;
        private final Element mappingElement;
        private final Number value;

        public EvaluationResult(SeverityLevel level, Element mappingElement, Number actualValue) {
            super();
            this.level = level;
            this.mappingElement = mappingElement;
            this.value = actualValue;
        }

        public SeverityLevel getLevel() {
            return level;
        }

        public Element getMappingElement() {
            return mappingElement;
        }

        public Number getValue() {
            return value;
        }

        public Number getThreshold() {
            if (mappingElement != null) {
                return mappingElement.getThreshold();
            } else {
                return null;
            }
        }

        public String getName() {
            if (level != null) {
                return level.getName();
            } else {
                return null;
            }
        }

        public String getId() {
            if (level != null) {
                return level.getId();
            } else {
                return null;
            }
        }

        @Override
        public String toString() {
            return "EvaluationResult [level=" + level + ", mappingElement=" + mappingElement + ", actualValue=" + value + "]";
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("level", level);
            builder.field("level_numeric", level != null ? level.getLevel() : 0);
            builder.field("mapping_element", mappingElement);
            builder.field("value", value);
            builder.field("threshold", getThreshold());
            builder.endObject();
            return builder;
        }

        public Map<String, Object> toMap() {
            HashMap<String, Object> map = new HashMap<>();
            map.put("level", level != null ? level.toMap() : null);
            map.put("name", getName());
            map.put("id", getId());
            map.put("mapping_element", mappingElement != null ? mappingElement.toMap() : null);
            map.put("value", value);
            map.put("threshold", getThreshold());
            return map;
        }

        public static EvaluationResult create(JsonNode jsonNode) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode, validationErrors);

            SeverityLevel level = vJsonNode.requiredCaseInsensitiveEnum("level", SeverityLevel.class);
            Number actualValue = vJsonNode.decimalValue("value", null);

            Element mappingElement = null;

            if (jsonNode.hasNonNull("mapping_element")) {
                try {
                    mappingElement = Element.create(jsonNode.get("mapping_element"));
                } catch (ConfigValidationException e) {
                    validationErrors.add("mapping_element", e);
                }
            }

            validationErrors.throwExceptionForPresentErrors();

            return new EvaluationResult(level, mappingElement, actualValue);
        }
    }

    public List<Element> getMapping() {
        return mapping;
    }

    public static final SeverityMapping DUMMY_MAPPING = new SeverityMapping(null, null, Order.ASCENDING,
            Arrays.asList(new Element(new BigDecimal("1"), SeverityLevel.INFO), new Element(new BigDecimal("2"), SeverityLevel.WARNING),
                    new Element(new BigDecimal("3"), SeverityLevel.ERROR), new Element(new BigDecimal("4"), SeverityLevel.CRITICAL)));

}
