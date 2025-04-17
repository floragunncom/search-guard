/*
 * Copyright 2022 floragunn GmbH
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

package com.floragunn.searchsupport;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;

import com.floragunn.codova.config.text.Pattern;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;

/**
 * Platform-independent abstraction for the Settings class
 */
public class StaticSettings {
    public static final StaticSettings EMPTY = new StaticSettings(Settings.EMPTY, null);
    private static final Logger log = LogManager.getLogger(StaticSettings.class);

    private final org.elasticsearch.common.settings.Settings settings;
    private final org.elasticsearch.env.Environment environment;
    private final Path configPath;

    public StaticSettings(org.elasticsearch.common.settings.Settings settings, Path configPath) {
        this.settings = settings;
        this.environment = configPath != null ? new Environment(settings, configPath) : null;
        this.configPath = configPath;
    }

    public Path getPlatformPluginsDirectory() {
        return this.environment.pluginsDir();
    }
    
    public Path getPatformConfigDirectory() {
        return this.environment.configDir();
    }

    public <V> V get(Attribute<V> option) {
        return option.getFrom(settings);
    }

    public org.elasticsearch.common.settings.Settings getPlatformSettings() {
        return settings;
    }

    public Path getConfigPath() {
        return configPath;
    }

    public abstract static class Attribute<V> {
        public static Builder<Object> define(String name) {
            return new Builder<Object>(name);
        }

        protected final String name;
        protected final V defaultValue;
        protected final boolean filtered;
        protected final org.elasticsearch.common.settings.Setting<?> platformInstance;

        Attribute(String name, V defaultValue, boolean filtered) {
            this.name = name;
            this.defaultValue = defaultValue;
            this.filtered = filtered;
            this.platformInstance = toPlatformInstance();
        }

        @SuppressWarnings("unchecked")
        V getFrom(org.elasticsearch.common.settings.Settings settings) {
            return (V) platformInstance.get(settings);
        }

        public String name() {
            return name;
        }

        protected abstract org.elasticsearch.common.settings.Setting<?> toPlatformInstance();

        protected org.elasticsearch.common.settings.Setting.Property[] toPlatformProperties() {
            List<Property> result = new ArrayList<>(3);

            result.add(Property.NodeScope);

            if (filtered) {
                result.add(Property.Filtered);
            }

            return result.toArray(new org.elasticsearch.common.settings.Setting.Property[result.size()]);
        }

        public static class Builder<V> {
            private final String name;
            private V defaultValue = null;
            private boolean filtered = false;

            Builder(String name) {
                this.name = name;
            }

            public Builder<V> filterValueFromUI() {
                this.filtered = true;
                return this;
            }

            public StringBuilder withDefault(String defaultValue) {
                @SuppressWarnings({ "unchecked", "rawtypes" })
                Builder<String> castedBuilder = (Builder<String>) (Builder) this;
                castedBuilder.defaultValue = defaultValue;
                return new StringBuilder(castedBuilder);
            }

            public IntegerBuilder withDefault(int defaultValue) {
                @SuppressWarnings({ "unchecked", "rawtypes" })
                Builder<Integer> castedBuilder = (Builder<Integer>) (Builder) this;
                castedBuilder.defaultValue = defaultValue;
                return new IntegerBuilder(castedBuilder);
            }

            public BooleanBuilder withDefault(boolean defaultValue) {
                @SuppressWarnings({ "unchecked", "rawtypes" })
                Builder<Boolean> castedBuilder = (Builder<Boolean>) (Builder) this;
                castedBuilder.defaultValue = defaultValue;
                return new BooleanBuilder(castedBuilder);
            }

            public TimeValueBuilder withDefault(TimeValue defaultValue) {
                @SuppressWarnings({ "unchecked", "rawtypes" })
                Builder<TimeValue> castedBuilder = (Builder<TimeValue>) (Builder) this;
                castedBuilder.defaultValue = defaultValue;
                return new TimeValueBuilder(castedBuilder);
            }

            public PatternBuilder withDefault(Pattern defaultValue) {
                @SuppressWarnings({ "unchecked", "rawtypes" })
                Builder<Pattern> castedBuilder = (Builder<Pattern>) (Builder) this;
                castedBuilder.defaultValue = defaultValue;
                return new PatternBuilder(castedBuilder);
            }

            public ByteSizeValueBuilder withDefault(ByteSizeValue defaultValue) {
                @SuppressWarnings({ "unchecked", "rawtypes" })
                Builder<ByteSizeValue> castedBuilder = (Builder<ByteSizeValue>) (Builder) this;
                castedBuilder.defaultValue = defaultValue;
                return new ByteSizeValueBuilder(castedBuilder);
            }
            
            public StringListAttribute asListOfStrings() {
                return new StringListAttribute(name, ImmutableList.empty(), filtered);
            }

        }

        public static class StringBuilder {
            private final Builder<String> parent;

            StringBuilder(Builder<String> parent) {
                this.parent = parent;
            }

            public Attribute<String> asString() {
                return new StringAttribute(parent.name, parent.defaultValue, parent.filtered);
            }
        }

        public static class BooleanBuilder {
            private final Builder<Boolean> parent;

            BooleanBuilder(Builder<Boolean> parent) {
                this.parent = parent;
            }

            public Attribute<Boolean> asBoolean() {
                return new BooleanAttribute(parent.name, parent.defaultValue, parent.filtered);
            }
        }

        public static class IntegerBuilder {
            private final Builder<Integer> parent;

            IntegerBuilder(Builder<Integer> parent) {
                this.parent = parent;
            }

            public Attribute<Integer> asInteger() {
                return new IntegerAttribute(parent.name, parent.defaultValue, parent.filtered);
            }
        }

        public static class TimeValueBuilder {
            private final Builder<TimeValue> parent;

            TimeValueBuilder(Builder<TimeValue> parent) {
                this.parent = parent;
            }

            public Attribute<TimeValue> asTimeValue() {
                return new TimeValueAttribute(parent.name, parent.defaultValue, parent.filtered);
            }
        }

        public static class PatternBuilder {
            private final Builder<Pattern> parent;

            PatternBuilder(Builder<Pattern> parent) {
                this.parent = parent;
            }

            public Attribute<Pattern> asPattern() {
                return new PatternAttribute(parent.name, parent.defaultValue, parent.filtered);
            }
        }

        public static class ByteSizeValueBuilder {
            private final Builder<ByteSizeValue> parent;

            ByteSizeValueBuilder(Builder<ByteSizeValue> parent) {
                this.parent = parent;
            }

            public Attribute<ByteSizeValue> asByteSizeValue() {
                return new ByteSizeValueAttribute(parent.name, parent.defaultValue, parent.filtered);
            }
        }
    }

    static class StringAttribute extends Attribute<String> {
        StringAttribute(String name, String defaultValue, boolean filtered) {
            super(name, defaultValue, filtered);
        }

        @Override
        protected org.elasticsearch.common.settings.Setting<String> toPlatformInstance() {
            if (defaultValue == null) {
                return org.elasticsearch.common.settings.Setting.simpleString(name, toPlatformProperties());
            } else {
                return org.elasticsearch.common.settings.Setting.simpleString(name, defaultValue, toPlatformProperties());
            }
        }
    }

    static class IntegerAttribute extends Attribute<Integer> {
        IntegerAttribute(String name, Integer defaultValue, boolean filtered) {
            super(name, defaultValue, filtered);
        }

        @Override
        protected org.elasticsearch.common.settings.Setting<Integer> toPlatformInstance() {
            return org.elasticsearch.common.settings.Setting.intSetting(name, defaultValue != null ? defaultValue : 0, toPlatformProperties());
        }
    }

    static class BooleanAttribute extends Attribute<Boolean> {
        BooleanAttribute(String name, Boolean defaultValue, boolean filtered) {
            super(name, defaultValue, filtered);
        }

        @Override
        protected org.elasticsearch.common.settings.Setting<Boolean> toPlatformInstance() {
            return org.elasticsearch.common.settings.Setting.boolSetting(name, defaultValue != null ? defaultValue : false, toPlatformProperties());
        }
    }

    static class TimeValueAttribute extends Attribute<TimeValue> {
        TimeValueAttribute(String name, TimeValue defaultValue, boolean filtered) {
            super(name, defaultValue, filtered);
        }

        @Override
        protected org.elasticsearch.common.settings.Setting<TimeValue> toPlatformInstance() {
            return org.elasticsearch.common.settings.Setting.timeSetting(name, defaultValue, TimeValue.timeValueSeconds(1), toPlatformProperties());
        }
    }

    static class PatternAttribute extends Attribute<Pattern> {
        private static final List<String> EMPTY_DEFAULT = ImmutableList.of("___empty");

        PatternAttribute(String name, Pattern defaultValue, boolean filtered) {
            super(name, defaultValue, filtered);
        }

        @Override
        protected org.elasticsearch.common.settings.Setting<?> toPlatformInstance() {
            return org.elasticsearch.common.settings.Setting.listSetting(name, EMPTY_DEFAULT, Function.identity(), toPlatformProperties());
        }

        @Override
        Pattern getFrom(Settings settings) {
            @SuppressWarnings("unchecked")
            List<String> value = (List<String>) platformInstance.get(settings);
            if (value.equals(EMPTY_DEFAULT)) {
                return defaultValue;
            } else {
                try {
                    return Pattern.create(value);
                } catch (ConfigValidationException e) {
                    log.error("Invalid pattern value for setting " + name(), e);
                    return defaultValue;
                }
            }
        }
    }

    static class StringListAttribute extends Attribute<List<String>> {
        StringListAttribute(String name, List<String> defaultValue, boolean filtered) {
            super(name, defaultValue, filtered);
        }

        @Override
        protected org.elasticsearch.common.settings.Setting<?> toPlatformInstance() {
            return org.elasticsearch.common.settings.Setting.listSetting(name, defaultValue, Function.identity(), toPlatformProperties());
        }
    }


    static class ByteSizeValueAttribute extends Attribute<ByteSizeValue> {
        ByteSizeValueAttribute(String name, ByteSizeValue defaultValue, boolean filtered) {
            super(name, defaultValue, filtered);
        }

        @Override
        protected org.elasticsearch.common.settings.Setting<ByteSizeValue> toPlatformInstance() {
            return org.elasticsearch.common.settings.Setting.memorySizeSetting(name, defaultValue, toPlatformProperties());
        }
    }
    
    public static class AttributeSet {
        private static final AttributeSet EMPTY = new AttributeSet(ImmutableList.empty());

        public static AttributeSet of(Attribute<?>... options) {
            return new AttributeSet(ImmutableList.ofArray(options));
        }

        public static AttributeSet empty() {
            return EMPTY;
        }

        private final ImmutableList<Attribute<?>> options;

        AttributeSet(ImmutableList<Attribute<?>> options) {
            this.options = options;
        }

        public ImmutableList<org.elasticsearch.common.settings.Setting<?>> toPlatform() {
            return options.map(Attribute::toPlatformInstance);
        }

    }
}
