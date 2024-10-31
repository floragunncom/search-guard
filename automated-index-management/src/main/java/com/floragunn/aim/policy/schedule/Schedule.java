package com.floragunn.aim.policy.schedule;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableMap;
import org.apache.commons.codec.digest.DigestUtils;
import org.quartz.JobKey;
import org.quartz.Trigger;
import org.quartz.TriggerKey;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class Schedule implements Document<Object> {
    private final Scope scope;

    public Schedule(Scope scope) {
        this.scope = scope;
    }

    public abstract Trigger buildTrigger(JobKey jobKey);

    protected abstract Map<String, Object> toBasicMap();

    protected abstract String getType();

    protected abstract String getStringRepresentation();

    @Override
    public final Object toBasicObject() {
        return ImmutableMap.of(getType(), toBasicMap());
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof Schedule && ((Schedule) other).scope == scope;
    }

    public Scope getScope() {
        return scope;
    }

    public TriggerKey getTriggerKey(JobKey jobKey) {
        String prefix = scope.getPrefix() + '_';
        return new TriggerKey(prefix + jobKey.getName() + "___" + DigestUtils.md5Hex(getStringRepresentation()), jobKey.getGroup());
    }

    public static class Factory {
        public static Factory defaultFactory() {
            Factory factory = new Factory();
            factory.register(IntervalSchedule.TYPE, IntervalSchedule::new);
            return factory;
        }

        private final Map<String, Parser> registry;

        private Factory() {
            registry = new ConcurrentHashMap<>();
        }

        public Schedule parse(DocNode docNode, Scope scope) throws ConfigValidationException {
            ValidationErrors errors = new ValidationErrors();
            ValidatingDocNode node = new ValidatingDocNode(docNode, errors);
            Schedule schedule = null;
            for (String type : registry.keySet()) {
                if (node.hasNonNull(type)) {
                    schedule = registry.get(type).parse(node.getAsDocNode(type), scope);
                    break;
                }
            }
            if (schedule == null) {
                errors.add(new ValidationError("schedule", "Empty schedule not allowed"));
            }
            node.checkForUnusedAttributes();
            errors.throwExceptionForPresentErrors();
            return schedule;
        }

        public void register(String type, Parser parser) {
            if (registry.containsKey(type)) {
                throw new IllegalArgumentException("Action of type '" + type + "' is already registered");
            }
            registry.put(type, parser);
        }

        public boolean containsType(String type) {
            return registry.containsKey(type);
        }
    }

    public interface Parser {
        Schedule parse(DocNode docNode, Scope scope) throws ConfigValidationException;
    }

    public enum Scope {
        POLICY("policy"), STEP("step"), DEFAULT("default");

        private final String prefix;

        Scope(String prefix) {
            this.prefix = prefix;
        }

        public String getPrefix() {
            return prefix;
        }
    }
}
