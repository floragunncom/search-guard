package com.floragunn.aim;

import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.actions.Action;
import com.floragunn.aim.policy.conditions.Condition;
import com.floragunn.aim.policy.instance.PolicyInstance;
import com.floragunn.aim.policy.instance.PolicyInstanceState;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.fluent.collections.ImmutableMap;

import java.util.AbstractMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class MockSupport {
    public static final MockCondition STATE_LOG_ROLLOVER_DOC_COUNT = new MockCondition("2b874dbf-7e46-45e3-bca3-faa43011eed2");
    public static final MockCondition STATE_LOG_ROLLOVER_MAX_SIZE = new MockCondition("2f73b82f-d670-4e58-abd7-45501da3d9ce");
    public static final MockCondition STATE_LOG_DELETE_MAX_AGE = new MockCondition("95d1addf-bb29-4c94-80e3-5c8dbf056a18");

    public static void init() {
        if (!AutomatedIndexManagement.CONDITION_FACTORY.containsType(MockCondition.TYPE)) {
            AutomatedIndexManagement.CONDITION_FACTORY.register(MockCondition.TYPE, MockCondition.VALIDATING_PARSER);
        }
        if (!AutomatedIndexManagement.ACTION_FACTORY.containsType(MockAction.TYPE)) {
            AutomatedIndexManagement.ACTION_FACTORY.register(MockAction.TYPE, MockAction.VALIDATING_PARSER);
        }
    }

    public static class MockCondition extends Condition {
        public static final String TYPE = "mock_condition";
        public static final ValidatingParser VALIDATING_PARSER = new ValidatingParser() {
            @Override
            public Condition parse(ValidatingDocNode node, ValidationErrors errors, Policy.ValidationContext validationContext) {
                return new MockCondition(node.get("uuid").required().asString());
            }

            @Override
            public void validateType(Validator.TypeValidator typeValidator) {

            }
        };
        private static final Map<String, Map.Entry<Configuration, Map<String, Configuration>>> CONFIGURATION = new ConcurrentHashMap<>();

        private final String uuid;

        public MockCondition() {
            uuid = UUID.randomUUID().toString();
            CONFIGURATION.put(uuid, new AbstractMap.SimpleEntry<>(new Configuration(), new ConcurrentHashMap<>()));
        }

        private MockCondition(String uuid) {
            this.uuid = uuid;
            if (!CONFIGURATION.containsKey(uuid)) {
                CONFIGURATION.put(uuid, new AbstractMap.SimpleEntry<>(new MockCondition.Configuration(), new ConcurrentHashMap<>()));
            }
        }

        @Override
        public boolean execute(String index, PolicyInstance.ExecutionContext executionContext, PolicyInstanceState state) throws Exception {
            Configuration configuration = CONFIGURATION.get(uuid).getKey();
            configuration.executionCount++;
            Boolean fail = configuration.fail;
            Boolean result = configuration.result;

            Configuration indexConfiguration = getConfiguration(index);
            indexConfiguration.executionCount++;

            if (indexConfiguration.fail != null) {
                fail = indexConfiguration.fail;
            }
            if (indexConfiguration.result != null) {
                result = indexConfiguration.result;
            }
            if (fail != null && fail) {
                throw new IllegalStateException("error");
            }
            if (result != null) {
                return result;
            }
            return false;
        }

        public int getExecutionCount() {
            return CONFIGURATION.get(uuid).getKey().executionCount;
        }

        public MockCondition setResult(boolean result) {
            CONFIGURATION.get(uuid).getKey().result = result;
            return this;
        }

        public MockCondition setFail(boolean fail) {
            CONFIGURATION.get(uuid).getKey().fail = fail;
            return this;
        }

        public int getExecutionCount(String index) {
            return getConfiguration(index).executionCount;
        }

        public MockCondition setResult(String index, boolean result) {
            getConfiguration(index).result = result;
            return this;
        }

        public MockCondition setFail(String index, boolean fail) {
            getConfiguration(index).fail = fail;
            return this;
        }

        private Configuration getConfiguration(String index) {
            return CONFIGURATION.get(uuid).getValue().computeIfAbsent(index, k -> new Configuration());
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other instanceof MockCondition) {
                return uuid.equals(((MockCondition) other).uuid);
            }
            return false;
        }

        @Override
        public String getType() {
            return TYPE;
        }

        @Override
        public ImmutableMap<String, Object> configToBasicMap() {
            return ImmutableMap.of("uuid", uuid);
        }

        private static class Configuration {
            private Boolean result = null;
            private Boolean fail = null;
            private int executionCount = 0;
        }
    }

    public static class MockAction extends Action {
        private static final Map<String, Map.Entry<Configuration, Map<String, Configuration>>> CONFIGURATION = new ConcurrentHashMap<>();
        private static final ValidatingParser VALIDATING_PARSER = new ValidatingParser() {
            @Override
            public Action parse(ValidatingDocNode node, ValidationErrors errors, Policy.ValidationContext validationContext) {
                return new MockAction(node.get("uuid").required().asString());
            }

            @Override
            public void validateType(Validator.TypeValidator typeValidator) {

            }
        };
        public static final String TYPE = "mock_action";
        private final String uuid;

        public MockAction() {
            uuid = UUID.randomUUID().toString();
            CONFIGURATION.put(uuid, new AbstractMap.SimpleEntry<>(new Configuration(), new ConcurrentHashMap<>()));
        }

        private MockAction(String uuid) {
            this.uuid = uuid;
            if (!CONFIGURATION.containsKey(uuid)) {
                CONFIGURATION.put(uuid, new AbstractMap.SimpleEntry<>(new Configuration(), new ConcurrentHashMap<>()));
            }
        }

        public MockAction setFail(boolean fail) {
            CONFIGURATION.get(uuid).getKey().fail = fail;
            return this;
        }

        public int getExecutionCount() {
            return CONFIGURATION.get(uuid).getKey().executionCount;
        }

        public MockAction setFail(String index, boolean fail) {
            getConfiguration(index).fail = fail;
            return this;
        }

        public int getExecutionCount(String index) {
            return getConfiguration(index).executionCount;
        }

        public MockAction setRunnable(Runnable runnable) {
            CONFIGURATION.get(uuid).getKey().runnable = runnable;
            return this;
        }

        public MockAction setRunnable(String index, Runnable runnable) {
            getConfiguration(index).runnable = runnable;
            return this;
        }

        private Configuration getConfiguration(String index) {
            return CONFIGURATION.get(uuid).getValue().computeIfAbsent(index, k -> new Configuration());
        }

        @Override
        public void execute(String index, PolicyInstance.ExecutionContext executionContext, PolicyInstanceState state) throws Exception {
            Configuration configuration = CONFIGURATION.get(uuid).getKey();
            configuration.executionCount++;
            Runnable runnable = configuration.runnable;
            Boolean fail = configuration.fail;

            Configuration indexConfiguration = getConfiguration(index);

            indexConfiguration.executionCount++;
            if (indexConfiguration.runnable != null) {
                runnable = indexConfiguration.runnable;
            }
            if (indexConfiguration.fail != null) {
                fail = indexConfiguration.fail;
            }

            if (runnable != null) {
                runnable.run();
            }
            if (fail != null && fail) {
                throw new IllegalStateException("error");
            }
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other instanceof MockAction) {
                return uuid.equals(((MockAction) other).uuid);
            }
            return false;
        }

        @Override
        public String getType() {
            return TYPE;
        }

        @Override
        public ImmutableMap<String, Object> configToBasicMap() {
            return ImmutableMap.of("uuid", uuid);
        }

        private static class Configuration {
            int executionCount = 0;
            Boolean fail = null;
            Runnable runnable = null;
        }
    }
}
