package com.floragunn.signals.settings;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.config.validation.ValidationError;
import com.floragunn.searchsupport.config.validation.ValidationErrors;
import com.floragunn.searchsupport.util.temporal.ConstantDurationExpression;
import com.floragunn.searchsupport.util.temporal.DurationExpression;
import com.floragunn.signals.SignalsInitializationException;
import com.floragunn.signals.actions.settings.update.SettingsUpdateAction;
import com.floragunn.signals.support.LuckySisyphos;

public class SignalsSettings {
    private static final Logger log = LogManager.getLogger(SignalsSettings.class);

    private final DynamicSettings dynamicSettings;
    private final StaticSettings staticSettings;

    public SignalsSettings(Settings staticSettings) {
        this.staticSettings = new StaticSettings(staticSettings);
        this.dynamicSettings = new DynamicSettings(this.staticSettings.getIndexNames().getSettings(), this.staticSettings);
    }

    public DurationExpression getDefaultThrottlePeriod() {
        return dynamicSettings.getDefaultThrottlePeriod();
    }

    public Tenant getTenant(String tenant) {
        return dynamicSettings.getTenant(tenant);
    }

    public boolean isIncludeNodeInWatchLogEnabled() {
        return dynamicSettings.isIncludeNodeInWatchLogEnabled();
    }

    public void refresh(Client client) throws SignalsInitializationException {
        this.dynamicSettings.refresh(client);
    }

    public void addChangeListener(ChangeListener changeListener) {
        this.dynamicSettings.addChangeListener(changeListener);
    }

    public void removeChangeListener(ChangeListener changeListener) {
        this.dynamicSettings.removeChangeListener(changeListener);
    }

    public void update(Client client, String key, Object value) throws ConfigValidationException {
        this.dynamicSettings.update(client, key, value);
    }

    public DynamicSettings getDynamicSettings() {
        return dynamicSettings;
    }

    public StaticSettings getStaticSettings() {
        return staticSettings;
    }

    public static class DynamicSettings {

        public static final Setting<Boolean> ACTIVE = Setting.boolSetting("active", Boolean.TRUE);
        // TODO find reasonable default
        public static Setting<String> DEFAULT_THROTTLE_PERIOD = Setting.simpleString("execution.default_throttle_period", "10s");
        public static Setting<Boolean> INCLUDE_NODE_IN_WATCHLOG = Setting.boolSetting("watch_log.include_node", Boolean.TRUE);
        public static Setting<String> WATCH_LOG_INDEX = Setting.simpleString("watch_log.index");
        public static Setting<Settings> TENANT = Setting.groupSetting("tenant.");
        public static Setting<String> INTERNAL_AUTH_TOKEN_SIGNING_KEY = Setting.simpleString("internal_auth.token_signing_key");
        public static Setting<String> INTERNAL_AUTH_TOKEN_ENCRYPTION_KEY = Setting.simpleString("internal_auth.token_encryption_key");

        public static Setting<List<String>> ALLOWED_HTTP_ENDPOINTS = Setting.listSetting("http.allowed_endpoints", Collections.singletonList("*"),
                Function.identity());
        public static final Setting<String> NODE_FILTER = Setting.simpleString("node_filter");

        private final String indexName;
        private final StaticSettings staticSettings;

        private volatile Settings settings = Settings.builder().build();
        private volatile DurationExpression defaultThrottlePeriod;

        private Collection<ChangeListener> changeListeners = Collections.newSetFromMap(new ConcurrentHashMap<ChangeListener, Boolean>());

        DynamicSettings(String indexName, StaticSettings staticSettings) {
            this.indexName = indexName;
            this.staticSettings = staticSettings;
        }

        public boolean isActive() {
            return ACTIVE.get(settings);
        }

        DurationExpression getDefaultThrottlePeriod() {
            return defaultThrottlePeriod;
        }

        boolean isIncludeNodeInWatchLogEnabled() {
            return INCLUDE_NODE_IN_WATCHLOG.get(settings);
        }

        public String getInternalAuthTokenSigningKey() {
            String result = INTERNAL_AUTH_TOKEN_SIGNING_KEY.get(settings);

            if (result.isEmpty()) {
                return null;
            } else {
                return result;
            }
        }

        String getNodeFilter() {
            return NODE_FILTER.get(settings);
        }

        public String getInternalAuthTokenEncryptionKey() {
            String result = INTERNAL_AUTH_TOKEN_ENCRYPTION_KEY.get(settings);

            if (result.isEmpty()) {
                return null;
            } else {
                return result;
            }
        }

        public String getWatchLogIndex() {
            String result = WATCH_LOG_INDEX.get(settings);

            if (result == null || result.length() == 0) {
                result = staticSettings.getIndexNames().getLog();
            }

            return result;
        }

        public List<String> getAllowedHttpEndpoints() {
            return ALLOWED_HTTP_ENDPOINTS.get(settings);
        }

        Tenant getTenant(String name) {
            return new Tenant(settings.getAsSettings("tenant." + name), this);
        }

        void addChangeListener(ChangeListener changeListener) {
            this.changeListeners.add(changeListener);
        }

        void removeChangeListener(ChangeListener changeListener) {
            this.changeListeners.remove(changeListener);
        }

        private void initDefaultThrottlePeriod() {
            try {
                this.defaultThrottlePeriod = DurationExpression.parse(DEFAULT_THROTTLE_PERIOD.get(settings));
            } catch (Exception e) {
                log.error("Error parsing signals.execution.default_throttle_period: " + DEFAULT_THROTTLE_PERIOD.get(settings), e);
                this.defaultThrottlePeriod = new ConstantDurationExpression(Duration.ofSeconds(10));
            }
        }

        private void validate(String key, Object value) throws ConfigValidationException {
            ParsedSettingsKey parsedKey = matchSetting(key);

            Settings.Builder settingsBuilder = Settings.builder();

            if (value instanceof List) {
                settingsBuilder.putList(key, toStringList((List<?>) value));
            } else if (value != null) {
                settingsBuilder.put(key, String.valueOf(value));
            }

            Settings newSettings = settingsBuilder.build();

            if (!parsedKey.isGroup()) {
                try {
                    parsedKey.setting.get(newSettings);
                } catch (Exception e) {
                    throw new ConfigValidationException(new ValidationError(key, e.getMessage()).cause(e));
                }
            } else {
                Settings subSettings = newSettings.getAsSettings(parsedKey.setting.getKey() + parsedKey.groupName);
                try {
                    parsedKey.subSetting.get(subSettings);
                } catch (Exception e) {
                    throw new ConfigValidationException(new ValidationError(key, e.getMessage()).cause(e));
                }
            }

        }

        private void validate(String key, Object value, Object... moreKeyValue) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();

            try {
                validate(key, value);
            } catch (ConfigValidationException e) {
                validationErrors.add(null, e);
            }

            if (moreKeyValue != null) {
                for (int i = 0; i < moreKeyValue.length; i += 2) {
                    try {
                        validate(String.valueOf(moreKeyValue[i]), moreKeyValue[i + 1]);
                    } catch (ConfigValidationException e) {
                        validationErrors.add(null, e);
                    }
                }
            }

            validationErrors.throwExceptionForPresentErrors();
        }

        private List<String> toStringList(List<?> value) {
            List<String> result = new ArrayList<>(value.size());

            for (Object o : value) {
                result.add(String.valueOf(o));
            }

            return result;
        }

        public void update(Client client, String key, Object value) throws ConfigValidationException {

            validate(key, value);

            updateIndex(client, key, value);

            SettingsUpdateAction.send(client);
        }

        public void update(Client client, String key, Object value, Object... moreKeyValue) throws ConfigValidationException {
            validate(key, value, moreKeyValue);

            updateIndex(client, key, value);

            if (moreKeyValue != null) {
                for (int i = 0; i < moreKeyValue.length; i += 2) {
                    updateIndex(client, String.valueOf(moreKeyValue[i]), moreKeyValue[i + 1]);
                }
            }

            SettingsUpdateAction.send(client);
        }

        public void updateIndex(Client client, String key, Object value) throws ConfigValidationException {

            if (value != null) {
                String json;
                try {
                    json = DefaultObjectMapper.objectMapper.writeValueAsString(value);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }

                PrivilegedConfigClient.adapt(client)
                        .index(new IndexRequest(indexName).id(key).source("value", json).setRefreshPolicy(RefreshPolicy.IMMEDIATE)).actionGet();
            } else {
                PrivilegedConfigClient.adapt(client).delete(new DeleteRequest(indexName).id(key).setRefreshPolicy(RefreshPolicy.IMMEDIATE))
                        .actionGet();
            }
        }

        void refresh(Client client) throws SignalsInitializationException {
            try {
                SearchResponse response = LuckySisyphos.tryHard(() -> PrivilegedConfigClient.adapt(client)
                        .search(new SearchRequest(indexName).source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(1000)))
                        .actionGet());

                Settings.Builder newDynamicSettings = Settings.builder();

                for (SearchHit hit : response.getHits()) {
                    if (hit.getSourceAsMap().get("value") != null) {
                        try {
                            String json = hit.getSourceAsMap().get("value").toString();
                            JsonNode jsonNode = DefaultObjectMapper.readTree(json);

                            if (jsonNode.isArray()) {
                                List<String> list = new ArrayList<>();
                                for (JsonNode subNode : jsonNode) {
                                    list.add(subNode.asText());
                                }
                                newDynamicSettings.putList(hit.getId(), list);

                            } else {
                                newDynamicSettings.put(hit.getId(), jsonNode.asText());
                            }
                        } catch (Exception e) {
                            log.error("Error while parsing setting " + hit, e);
                        }
                    }
                }

                Settings newSettings = newDynamicSettings.build();

                if (!newSettings.equals(this.settings)) {
                    this.settings = newSettings;
                    notifyListeners();
                }
            } catch (IndexNotFoundException e) {
                log.info("Settings index does not exist yet");
            } catch (Exception e) {
                throw new SignalsInitializationException("Error while loading settings", e);
            } finally {
                initDefaultThrottlePeriod();
            }

        }

        private void notifyListeners() {
            for (ChangeListener listener : this.changeListeners) {
                try {
                    listener.onChange();
                } catch (Exception e) {
                    log.error("Error in " + listener, e);
                }
            }
        }

        static List<Setting<?>> getAvailableSettings() {
            return Arrays.asList(ACTIVE, DEFAULT_THROTTLE_PERIOD, INCLUDE_NODE_IN_WATCHLOG, ALLOWED_HTTP_ENDPOINTS, TENANT,
                    INTERNAL_AUTH_TOKEN_SIGNING_KEY, INTERNAL_AUTH_TOKEN_ENCRYPTION_KEY, WATCH_LOG_INDEX, NODE_FILTER);
        }

        public static Setting<?> getSetting(String key) throws ConfigValidationException {

            for (Setting<?> setting : getAvailableSettings()) {
                if (setting.match(key)) {
                    return setting;
                }
            }

            throw new ConfigValidationException(new ValidationError(key, "Unknown key"));
        }

        public static ParsedSettingsKey matchSetting(String key) throws ConfigValidationException {

            Setting<?> setting = getSetting(key);

            if (!setting.getKey().endsWith(".")) {
                return new ParsedSettingsKey(setting);
            } else {
                String remainingKey = key.substring(TENANT.getKey().length());
                int nextPeriod = remainingKey.indexOf('.');

                if (nextPeriod == -1 && nextPeriod == remainingKey.length() - 1) {
                    throw new ConfigValidationException(new ValidationError(key, "Illegal key"));
                }

                String groupName = remainingKey.substring(0, nextPeriod);
                String subSettingKey = remainingKey.substring(nextPeriod + 1);

                if (TENANT.match(key)) {
                    return new ParsedSettingsKey(setting, groupName, Tenant.getSetting(subSettingKey));
                } else {
                    throw new ConfigValidationException(new ValidationError(key, "Unknown key"));
                }
            }
        }

        public Settings getSettings() {
            return settings;
        }

    }

    public static class ParsedSettingsKey {
        public final Setting<?> setting;
        public final String groupName;
        public final Setting<?> subSetting;

        ParsedSettingsKey(Setting<?> setting) {
            this.setting = setting;
            this.groupName = null;
            this.subSetting = null;
        }

        ParsedSettingsKey(Setting<?> setting, String groupName, Setting<?> subSetting) {
            this.setting = setting;
            this.groupName = groupName;
            this.subSetting = subSetting;
        }

        public Setting<?> getSetting() {
            return setting;
        }

        public String getGroupName() {
            return groupName;
        }

        public Setting<?> getSubSetting() {
            return subSetting;
        }

        public boolean isGroup() {
            return groupName != null;
        }
    }

    public static class Tenant {
        static final Setting<String> NODE_FILTER = Setting.simpleString("node_filter");
        static final Setting<Boolean> ACTIVE = Setting.boolSetting("active", Boolean.TRUE);

        private final Settings settings;
        private final DynamicSettings parent;

        Tenant(Settings settings, DynamicSettings parent) {
            this.settings = settings;
            this.parent = parent;
        }

        public String getNodeFilter() {
            String result = NODE_FILTER.get(settings);

            if (result == null || result.isEmpty()) {
                result = parent.getNodeFilter();
            }

            if (result == null || result.isEmpty()) {
                return null;
            } else {
                return result;
            }
        }

        public boolean isActive() {
            return ACTIVE.get(settings);
        }

        public static Setting<?> getSetting(String key) {

            if (NODE_FILTER.match(key)) {
                return NODE_FILTER;
            } else if (ACTIVE.match(key)) {
                return ACTIVE;
            }

            throw new IllegalArgumentException("Unkown key: " + key);
        }
    }

    public static class StaticSettings {
        public static Setting<Boolean> ENABLED = Setting.boolSetting("signals.enabled", true, Property.NodeScope);
        public static Setting<Boolean> ENTERPRISE_ENABLED = Setting.boolSetting("signals.enterprise.enabled", true, Property.NodeScope);

        public static class IndexNames {
            public static Setting<String> WATCHES = Setting.simpleString("signals.index_names.watches", ".signals_watches", Property.NodeScope);
            public static Setting<String> WATCHES_STATE = Setting.simpleString("signals.index_names.watches_state", ".signals_watches_state",
                    Property.NodeScope);
            public static Setting<String> WATCHES_TRIGGER_STATE = Setting.simpleString("signals.index_names.watches_trigger_state",
                    ".signals_watches_trigger_state", Property.NodeScope);
            public static Setting<String> ACCOUNTS = Setting.simpleString("signals.index_names.accounts", ".signals_accounts", Property.NodeScope);
            public static Setting<String> SETTINGS = Setting.simpleString("signals.index_names.settings", ".signals_settings", Property.NodeScope);
            public static Setting<String> LOG = Setting.simpleString("signals.index_names.log", "<.signals_log_{now/d}>", Property.NodeScope);

            private final Settings settings;

            public IndexNames(Settings settings) {
                this.settings = settings;
            }

            public String getWatches() {
                return WATCHES.get(settings);
            }

            public String getWatchesState() {
                return WATCHES_STATE.get(settings);
            }

            public String getWatchesTriggerState() {
                return WATCHES_TRIGGER_STATE.get(settings);
            }

            public String getLog() {
                return LOG.get(settings);
            }

            public String getAccounts() {
                return ACCOUNTS.get(settings);
            }

            public String getSettings() {
                return SETTINGS.get(settings);
            }
        }

        public static List<Setting<?>> getAvailableSettings() {
            return Arrays.asList(ENABLED, ENTERPRISE_ENABLED, IndexNames.WATCHES, IndexNames.WATCHES_STATE, IndexNames.WATCHES_TRIGGER_STATE,
                    IndexNames.ACCOUNTS, IndexNames.LOG);
        }

        private final Settings settings;
        private final IndexNames indexNames;

        public StaticSettings(Settings settings) {
            this.settings = settings;
            this.indexNames = new IndexNames(settings);
        }

        public boolean isEnabled() {
            return ENABLED.get(settings);
        }

        public boolean isEnterpriseEnabled() {
            return ENTERPRISE_ENABLED.get(settings);
        }

        public IndexNames getIndexNames() {
            return indexNames;
        }

        public Settings getSettings() {
            return settings;
        }
    }

    public static interface ChangeListener {
        void onChange();
    }

}
