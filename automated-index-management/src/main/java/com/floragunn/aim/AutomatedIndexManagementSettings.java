package com.floragunn.aim;

import com.floragunn.aim.support.ParsingSupport;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.StaticSettings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public final class AutomatedIndexManagementSettings {
    private final Static staticSettings;
    private final Dynamic dynamicSettings;

    public AutomatedIndexManagementSettings(Settings settings) {
        staticSettings = new Static(settings);
        dynamicSettings = new Dynamic();
    }

    public Static getStatic() {
        return staticSettings;
    }

    public Dynamic getDynamic() {
        return dynamicSettings;
    }

    public static class Dynamic {
        private static final Logger LOG = LogManager.getLogger(Dynamic.class);
        public static final DynamicAttribute<Boolean> ACTIVE = DynamicAttribute.define("active").withDefault(Boolean.TRUE).asBoolean();
        public static final DynamicAttribute<Boolean> EXECUTION_DELAY_RANDOM_ENABLED = DynamicAttribute.define("execution.delay.random.enabled")
                .withDefault(Boolean.TRUE).asBoolean();
        public static final DynamicAttribute<TimeValue> EXECUTION_DELAY = DynamicAttribute.define("execution.delay").withDefault(TimeValue.ZERO)
                .asTimeValue();
        public static final DynamicAttribute<TimeValue> EXECUTION_PERIOD = DynamicAttribute.define("execution.period")
                .withDefault(TimeValue.timeValueMinutes(5)).asTimeValue();
        public static final DynamicAttribute<Boolean> STATE_LOG_ACTIVE = DynamicAttribute.define("state_log.active").withDefault(Boolean.TRUE)
                .asBoolean();

        public static List<DynamicAttribute<?>> getAvailableSettings() {
            return ImmutableList.of(ACTIVE, EXECUTION_DELAY_RANDOM_ENABLED, EXECUTION_DELAY, EXECUTION_PERIOD, STATE_LOG_ACTIVE);
        }

        public static DynamicAttribute<?> findAvailableSettingByKey(String key) {
            return getAvailableSettings().stream().filter(dynamicAttribute -> dynamicAttribute.getName().equals(key)).findFirst().orElse(null);
        }

        private final List<ChangeListener> changeListeners;
        private final Map<String, Object> settings;

        protected Dynamic() {
            this.changeListeners = new ArrayList<>();
            settings = new ConcurrentHashMap<>();
        }

        public void init(PrivilegedConfigClient client) {
            try {
                MultiGetResponse response = client.prepareMultiGet().addIds(ConfigIndices.SETTINGS_NAME,
                        getAvailableSettings().stream().map(DynamicAttribute::getName).collect(Collectors.toList())).execute().actionGet();
                for (MultiGetItemResponse item : response.getResponses()) {
                    if (!item.isFailed()) {
                        GetResponse getResponse = item.getResponse();
                        if (getResponse.isExists()) {
                            DynamicAttribute<?> attribute = findAvailableSettingByKey(item.getId());
                            Object basicObject = DocReader.json().read((String) getResponse.getSource().get("setting"));
                            Object value = attribute.fromBasicObject(basicObject);
                            settings.put(item.getId(), value);
                        }
                    } else {
                        LOG.warn("Failed to retrieve setting '{}' from settings index\n{}", item.getId(), item.getFailure().getFailure());
                    }
                }
            } catch (Exception e) {
                LOG.error("Error while initializing settings", e);
            }
        }

        public void addChangeListener(ChangeListener listener) {
            changeListeners.add(listener);
        }

        public void removeChangeListener(ChangeListener listener) {
            changeListeners.remove(listener);
        }

        public <T> T get(DynamicAttribute<T> attribute) {
            @SuppressWarnings("unchecked")
            T value = (T) settings.get(attribute.getName());
            if (value == null) {
                return attribute.getDefaultValue();
            }
            return value;
        }

        public boolean getActive() {
            return get(ACTIVE);
        }

        public boolean getExecutionRandomDelayEnabled() {
            return get(EXECUTION_DELAY_RANDOM_ENABLED);
        }

        public TimeValue getExecutionFixedDelay() {
            return get(EXECUTION_DELAY);
        }

        public TimeValue getExecutionPeriod() {
            return get(EXECUTION_PERIOD);
        }

        public boolean getStateLogActive() {
            return get(STATE_LOG_ACTIVE);
        }

        public void refresh(Map<DynamicAttribute<?>, Object> changed, List<DynamicAttribute<?>> deleted) {
            for (DynamicAttribute<?> attribute : deleted) {
                settings.remove(attribute.getName());
            }
            for (Map.Entry<DynamicAttribute<?>, Object> attributeEntry : changed.entrySet()) {
                settings.put(attributeEntry.getKey().getName(), attributeEntry.getValue());
            }
            for (ChangeListener listener : changeListeners) {
                List<DynamicAttribute<?>> allChanges = new ArrayList<>(changed.size() + deleted.size());
                allChanges.addAll(changed.keySet());
                allChanges.addAll(deleted);
                listener.onChange(allChanges);
            }
        }

        public interface ChangeListener {
            void onChange(List<DynamicAttribute<?>> changed);
        }

        public static class DynamicAttribute<V> {
            public static <T> Builder<T> define(String name) {
                return new Builder<>(name);
            }

            private final String name;
            private final V defaultValue;
            private final StringParser<V> parser;

            private DynamicAttribute(String name, V defaultValue, StringParser<V> parser) {
                this.name = name;
                this.defaultValue = defaultValue;
                this.parser = parser;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                DynamicAttribute<?> that = (DynamicAttribute<?>) o;
                return Objects.equals(getName(), that.getName());
            }

            @Override
            public int hashCode() {
                return Objects.hash(getName());
            }

            public String getName() {
                return name;
            }

            public V getDefaultValue() {
                return defaultValue;
            }

            public V fromBasicObject(Object obj) {
                return parser.fromBasicObject(obj);
            }

            public Object toBasicObject(Object value) {
                return parser.toBasicObject(value);
            }

            public void validate(Object value) throws ConfigValidationException {
                parser.validate(value);
            }

            public static class Builder<V> {
                private final String name;
                private V defaultValue;

                protected Builder(String name) {
                    this.name = name;
                    defaultValue = null;
                }

                private Builder(String name, V defaultValue) {
                    this.name = name;
                    this.defaultValue = defaultValue;
                }

                public Builder<V> withDefault(V defaultValue) {
                    this.defaultValue = defaultValue;
                    return this;
                }

                private DynamicAttribute<V> asGeneric(StringParser<V> parser) {
                    return new DynamicAttribute<>(name, defaultValue, parser);
                }

                public DynamicAttribute<String> asString() {
                    Builder<String> stringBuilder;
                    if (defaultValue == null) {
                        stringBuilder = new Builder<>(name, null);
                    } else if (defaultValue instanceof String) {
                        stringBuilder = new Builder<>(name, (String) defaultValue);
                    } else {
                        stringBuilder = new Builder<>(name, defaultValue.toString());
                    }
                    return stringBuilder.asGeneric(value -> {
                        if (!(value instanceof String)) {
                            throw new ConfigValidationException(new ValidationError(name, "Expected string value"));
                        }
                    });
                }

                public DynamicAttribute<Boolean> asBoolean() {
                    Builder<Boolean> booleanBuilder;
                    if (defaultValue == null) {
                        booleanBuilder = new Builder<>(name, null);
                    } else if (defaultValue instanceof Boolean) {
                        booleanBuilder = new Builder<>(name, (Boolean) defaultValue);
                    } else {
                        throw new IllegalArgumentException("Default value for '" + name + "' is not a boolean");
                    }
                    return booleanBuilder.asGeneric(value -> {
                        if (!(value instanceof Boolean)) {
                            throw new ConfigValidationException(new ValidationError(name, "Expected boolean value"));
                        }
                    });
                }

                public DynamicAttribute<TimeValue> asTimeValue() {
                    Builder<TimeValue> timeValueBuilder;
                    if (defaultValue == null) {
                        timeValueBuilder = new Builder<>(name, null);
                    } else if (defaultValue instanceof TimeValue) {
                        timeValueBuilder = new Builder<>(name, (TimeValue) defaultValue);
                    } else {
                        throw new IllegalArgumentException("Default value for '" + name + "' is not a time value");
                    }
                    return timeValueBuilder.asGeneric(new StringParser<>() {
                        @Override
                        public TimeValue fromBasicObject(Object obj) {
                            return TimeValue.parseTimeValue((String) obj, name);
                        }

                        @Override
                        public Object toBasicObject(Object value) {
                            return value.toString();
                        }

                        @Override
                        public void validate(Object value) throws ConfigValidationException {
                            if (!(value instanceof String)) {
                                throw new ConfigValidationException(new ValidationError(name, "Expected time value string " + value, value));
                            }
                            try {
                                ParsingSupport.timeValueParser((String) value);
                            } catch (IllegalArgumentException e) {
                                throw new ConfigValidationException(new ValidationError(name, "Invalid time value"));
                            }
                        }
                    });
                }
            }

            private interface StringParser<V> {
                default V fromBasicObject(Object obj) {
                    @SuppressWarnings("unchecked")
                    V value = (V) obj;
                    return value;
                }

                default Object toBasicObject(Object value) {
                    return value;
                }

                void validate(Object value) throws ConfigValidationException;
            }
        }
    }

    public static class Static {
        public static final Boolean DEFAULT_ENABLED = true;
        public static final Integer DEFAULT_THREAD_POOL_SIZE = 4;

        public static final StaticSettings.Attribute<Boolean> ENABLED = StaticSettings.Attribute.define("aim.enabled").withDefault(DEFAULT_ENABLED)
                .asBoolean();
        public static final StaticSettings.Attribute<Integer> THREAD_POOL_SIZE = StaticSettings.Attribute.define("aim.thread_pool.size")
                .withDefault(DEFAULT_THREAD_POOL_SIZE).asInteger();

        public static final StaticSettings.Attribute<String> POLICY_NAME_FIELD = StaticSettings.Attribute.define("index.aim.policy_name").index()
                .asString();
        public static final StaticSettings.Attribute<String> ROLLOVER_ALIAS_FIELD = StaticSettings.Attribute.define("index.aim.rollover_alias")
                .index().dynamic().asString();
        public static final StaticSettings.Attribute<Map<String, String>> ALIASES_FIELD = StaticSettings.Attribute.define("index.aim.alias_mapping")
                .index().dynamic().asMapOfStrings();

        public static StaticSettings.AttributeSet getAvailableSettings() {
            return StaticSettings.AttributeSet.of(ENABLED, THREAD_POOL_SIZE, POLICY_NAME_FIELD, ROLLOVER_ALIAS_FIELD, ALIASES_FIELD, StateLog.ENABLED,
                    StateLog.INDEX_TEMPLATE_NAME, StateLog.INDEX_NAME_PREFIX, StateLog.ALIAS_NAME, StateLog.POLICY_NAME);
        }

        private final StaticSettings settings;
        private final StateLog stateLog;

        protected Static(Settings settings) {
            this.settings = new StaticSettings(settings, null);
            stateLog = new StateLog(this.settings);
        }

        public int getThreadPoolSize() {
            return settings.get(THREAD_POOL_SIZE);
        }

        public String getPolicyName(Settings indexSettings) {
            return POLICY_NAME_FIELD.getFrom(indexSettings);
        }

        public String getRolloverAlias(Settings indexSettings) {
            return ROLLOVER_ALIAS_FIELD.getFrom(indexSettings);
        }

        public Map<String, String> getAliases(Settings indexSettings) {
            return ALIASES_FIELD.getFrom(indexSettings);
        }

        public StateLog stateLog() {
            return stateLog;
        }

        public static class StateLog {
            public static final Boolean DEFAULT_ENABLED = true;
            public static final String DEFAULT_INDEX_TEMPLATE_NAME = ".aim_state_log";
            public static final String DEFAULT_INDEX_NAME_PREFIX = ".aim_state_log";
            public static final String DEFAULT_ALIAS_NAME = ".aim_state_log";
            public static final String DEFAULT_POLICY_NAME = ".state_log_policy";

            public static final StaticSettings.Attribute<Boolean> ENABLED = StaticSettings.Attribute.define("aim.state_log.enabled")
                    .withDefault(DEFAULT_ENABLED).asBoolean();
            public static final StaticSettings.Attribute<String> INDEX_TEMPLATE_NAME = StaticSettings.Attribute
                    .define("aim.state_log.index.template.name").withDefault(DEFAULT_INDEX_TEMPLATE_NAME).asString();
            public static final StaticSettings.Attribute<String> INDEX_NAME_PREFIX = StaticSettings.Attribute
                    .define("aim.state_log.index.name.prefix").withDefault(DEFAULT_INDEX_NAME_PREFIX).asString();
            public static final StaticSettings.Attribute<String> ALIAS_NAME = StaticSettings.Attribute.define("aim.state_log.index.alias")
                    .withDefault(DEFAULT_ALIAS_NAME).asString();
            public static final StaticSettings.Attribute<String> POLICY_NAME = StaticSettings.Attribute.define("aim.state_log.policy.name")
                    .withDefault(DEFAULT_POLICY_NAME).asString();

            private final StaticSettings settings;

            protected StateLog(StaticSettings settings) {
                this.settings = settings;
            }

            public boolean isEnabled() {
                return settings.get(ENABLED);
            }

            public String getIndexTemplateName() {
                return settings.get(INDEX_TEMPLATE_NAME);
            }

            public String getIndexNamePrefix() {
                return settings.get(INDEX_NAME_PREFIX);
            }

            public String getAliasName() {
                return settings.get(ALIAS_NAME);
            }

            public String getPolicyName() {
                return settings.get(POLICY_NAME);
            }
        }
    }

    public static class ConfigIndices {
        public static final String SETTINGS_NAME = ".aim_settings";
        public static final String POLICIES_NAME = ".aim_policies";
        public static final String POLICY_INSTANCE_STATES_NAME = ".aim_states";
    }
}
