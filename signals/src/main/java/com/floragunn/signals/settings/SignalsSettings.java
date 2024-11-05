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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.floragunn.codova.config.temporal.ConstantDurationExpression;
import com.floragunn.codova.config.temporal.DurationExpression;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocWriter;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.StaticSettings;
import com.floragunn.signals.SignalsInitializationException;
import com.floragunn.signals.actions.settings.update.SettingsUpdateAction;
import com.floragunn.signals.support.LuckySisyphos;
import com.floragunn.signals.watch.common.HttpProxyConfig;

public class SignalsSettings {
    private static final Logger log = LogManager.getLogger(SignalsSettings.class);

    private final DynamicSettings dynamicSettings;
    private final SignalsStaticSettings staticSettings;

    public SignalsSettings(Settings staticSettings) {
        this.staticSettings = new SignalsStaticSettings(staticSettings);
        this.dynamicSettings = new DynamicSettings(this.staticSettings.getIndexNames().getSettings(), this.staticSettings);
    }

    public DurationExpression getDefaultThrottlePeriod() {
        return dynamicSettings.getDefaultThrottlePeriod();
    }

    public DurationExpression getThrottlePeriodLowerBound() {
        return dynamicSettings.getThrottlePeriodLowerBound();
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

    public SignalsStaticSettings getStaticSettings() {
        return staticSettings;
    }

    public static class DynamicSettings {

        public static final Setting<Boolean> ACTIVE = Setting.boolSetting("active", Boolean.TRUE);
        // TODO find reasonable default
        public static Setting<String> DEFAULT_THROTTLE_PERIOD = Setting.simpleString("execution.default_throttle_period", "10s");
        public static Setting<String> THROTTLE_PERIOD_LOWER_BOUND = Setting.simpleString("execution.throttle_period_lower_bound");
        public static Setting<Boolean> INCLUDE_NODE_IN_WATCHLOG = Setting.boolSetting("watch_log.include_node", Boolean.TRUE);
        public static Setting<String> WATCH_LOG_INDEX = Setting.simpleString("watch_log.index");
        public static Setting<Settings> TENANT = Setting.groupSetting("tenant.");
        public static Setting<String> INTERNAL_AUTH_TOKEN_SIGNING_KEY = Setting.simpleString("internal_auth.token_signing_key");
        public static Setting<String> INTERNAL_AUTH_TOKEN_ENCRYPTION_KEY = Setting.simpleString("internal_auth.token_encryption_key");

        public static Setting<List<String>> ALLOWED_HTTP_ENDPOINTS = Setting.listSetting("http.allowed_endpoints", Collections.singletonList("*"),
                Function.identity());
        public static final Setting<String> HTTP_PROXY = Setting.simpleString("http.proxy");

        public static final Setting<String> NODE_FILTER = Setting.simpleString("node_filter");

        public static final Setting<String> FRONTEND_BASE_URL = Setting.simpleString("frontend_base_url");
        
        private final String indexName;
        private final SignalsStaticSettings staticSettings;

        private volatile Settings settings = Settings.builder().build();
        private volatile DurationExpression defaultThrottlePeriod;
        private volatile DurationExpression throttlePeriodLowerBound;

        private Collection<ChangeListener> changeListeners = Collections.newSetFromMap(new ConcurrentHashMap<ChangeListener, Boolean>());

        DynamicSettings(String indexName, SignalsStaticSettings staticSettings) {
            this.indexName = indexName;
            this.staticSettings = staticSettings;
        }

        public boolean isActive() {
            return ACTIVE.get(settings);
        }

        DurationExpression getDefaultThrottlePeriod() {
            return defaultThrottlePeriod;
        }

        DurationExpression getThrottlePeriodLowerBound() {
            return throttlePeriodLowerBound;
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

        public String getFrontendBaseUrl() {
            return FRONTEND_BASE_URL.get(settings);
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

        public HttpProxyConfig getHttpProxyConfig() {
            String proxy = HTTP_PROXY.get(settings);

            if (proxy == null || proxy.length() == 0) {
                return null;
            }

            try {
                return HttpProxyConfig.create(proxy);
            } catch (ConfigValidationException e) {
                log.error("Invalid proxy config " + proxy, e);
                return null;
            }
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

        private void initThrottlePeriodLowerBound() {
            if (THROTTLE_PERIOD_LOWER_BOUND.exists(settings)) {
                try {
                    this.throttlePeriodLowerBound = DurationExpression.parse(THROTTLE_PERIOD_LOWER_BOUND.get(settings));
                } catch (Exception e) {
                    log.error("Error parsing signals.{}: {}", THROTTLE_PERIOD_LOWER_BOUND.getKey(), THROTTLE_PERIOD_LOWER_BOUND.get(settings), e);
                    this.throttlePeriodLowerBound = null;
                }
            } else {
                this.throttlePeriodLowerBound = null;
            }
        }

        private void validate(String key, Object value) throws ConfigValidationException {
            if (key.equals(HTTP_PROXY.getKey()) && value != null) {
                try {
                    HttpProxyConfig.create(value.toString());
                } catch (ConfigValidationException e) {
                    throw new ConfigValidationException(new ValidationErrors().add(key, e));
                }
            }
            
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
                String json = DocWriter.json().writeAsString(value);
                
                PrivilegedConfigClient.adapt(client)
                        .index(new IndexRequest(indexName).id(key).source("value", json).setRefreshPolicy(RefreshPolicy.IMMEDIATE)).actionGet();
            } else {
                PrivilegedConfigClient.adapt(client).delete(new DeleteRequest(indexName).id(key).setRefreshPolicy(RefreshPolicy.IMMEDIATE))
                        .actionGet();
            }
        }

        void refresh(Client client) throws SignalsInitializationException {
            try {
                Settings.Builder newDynamicSettings = Settings.builder();
                SearchResponse response = LuckySisyphos.tryHard(() -> PrivilegedConfigClient.adapt(client)
                        .search(new SearchRequest(indexName).source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(1000)))
                        .actionGet());

                try {
                    for (SearchHit hit : response.getHits()) {
                        if (hit.getSourceAsMap().get("value") != null) {
                            try {
                                String json = hit.getSourceAsMap().get("value").toString();
                                DocNode jsonNode = DocNode.parse(Format.JSON).from(json);

                                if (jsonNode.isList()) {
                                    List<String> list = new ArrayList<>();
                                    for (DocNode subNode : jsonNode.toListOfNodes()) {
                                        list.add(subNode.toString());
                                    }
                                    newDynamicSettings.putList(hit.getId(), list);
                                } else {
                                    newDynamicSettings.put(hit.getId(), jsonNode.toString());
                                }
                            } catch (Exception e) {
                                log.error("Error while parsing setting " + hit, e);
                            }
                        }
                    }
                } finally {
                    response.decRef();
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
                initThrottlePeriodLowerBound();
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
            return Arrays.asList(ACTIVE, DEFAULT_THROTTLE_PERIOD, THROTTLE_PERIOD_LOWER_BOUND, INCLUDE_NODE_IN_WATCHLOG, ALLOWED_HTTP_ENDPOINTS, TENANT,
                    INTERNAL_AUTH_TOKEN_SIGNING_KEY, INTERNAL_AUTH_TOKEN_ENCRYPTION_KEY, WATCH_LOG_INDEX, NODE_FILTER, HTTP_PROXY, FRONTEND_BASE_URL);
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

        /**
         * Note that the default value of ACTIVE is actually determined by the static setting signals.all_tenants_active_by_default 
         */
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
            return settings.getAsBoolean(ACTIVE.getKey(), parent.staticSettings.isActiveByDefault());
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

    public static class SignalsStaticSettings {
        public static StaticSettings.Attribute<Boolean> ENABLED = StaticSettings.Attribute.define("signals.enabled").withDefault(true).asBoolean();
        public static StaticSettings.Attribute<Boolean> ENTERPRISE_ENABLED = StaticSettings.Attribute.define("signals.enterprise.enabled")
                .withDefault(true).asBoolean();
        public static StaticSettings.Attribute<Integer> MAX_THREADS = StaticSettings.Attribute.define("signals.worker_threads.pool.max_size")
                .withDefault(5).asInteger();
        public static StaticSettings.Attribute<TimeValue> THREAD_KEEP_ALIVE = StaticSettings.Attribute
                .define("signals.worker_threads.pool.keep_alive").withDefault(TimeValue.timeValueMinutes(100)).asTimeValue();
        public static StaticSettings.Attribute<Integer> THREAD_PRIO =  StaticSettings.Attribute.define("signals.worker_threads.prio").withDefault(Thread.NORM_PRIORITY).asInteger();

        public static StaticSettings.Attribute<Boolean> ACTIVE_BY_DEFAULT =  StaticSettings.Attribute.define("signals.all_tenants_active_by_default").withDefault(true).asBoolean();
        public static StaticSettings.Attribute<String> WATCH_LOG_REFRESH_POLICY =  StaticSettings.Attribute.define("signals.watch_log.refresh_policy").withDefault((String) null).asString();
        public static StaticSettings.Attribute<Boolean> WATCH_LOG_SYNC_INDEXING =  StaticSettings.Attribute.define("signals.watch_log.sync_indexing").withDefault(false).asBoolean();
        public static StaticSettings.Attribute<Integer> WATCH_LOG_MAPPING_TOTAL_FIELDS_LIMIT =  StaticSettings.Attribute.define("signals.watch_log.mapping_total_fields_limit").withDefault(1000).asInteger();

        public static class IndexNames {

            public static final String TRUSTSTORES = ".signals_truststores";
            public static final String PROXIES = ".signals_proxies";
            public static StaticSettings.Attribute<String> WATCHES =  StaticSettings.Attribute.define("signals.index_names.watches").withDefault(".signals_watches").asString();
            public static StaticSettings.Attribute<String> WATCHES_STATE =  StaticSettings.Attribute.define("signals.index_names.watches_state").withDefault(".signals_watches_state").asString();
            public static StaticSettings.Attribute<String> WATCHES_TRIGGER_STATE =  StaticSettings.Attribute.define("signals.index_names.watches_trigger_state").withDefault(
                    ".signals_watches_trigger_state").asString();
            public static StaticSettings.Attribute<String> ACCOUNTS =  StaticSettings.Attribute.define("signals.index_names.accounts").withDefault(".signals_accounts").asString();
            public static StaticSettings.Attribute<String> SETTINGS =  StaticSettings.Attribute.define("signals.index_names.settings").withDefault(".signals_settings").asString();
            public static StaticSettings.Attribute<String> LOG = StaticSettings.Attribute.define("signals.index_names.log").withDefault("<.signals_log_{now/d}>").asString();

            private final StaticSettings settings;

            public IndexNames(Settings settings) {
                this.settings = new StaticSettings(settings, null);
            }

            public String getWatches() {
                return settings.get(WATCHES);
            }

            public String getWatchesState() {
                return settings.get(WATCHES_STATE);
            }

            public String getWatchesTriggerState() {
                return settings.get(WATCHES_TRIGGER_STATE);
            }

            public String getLog() {
                return settings.get(LOG);
            }

            public String getAccounts() {
                return settings.get(ACCOUNTS);
            }

            public String getSettings() {
                return settings.get(SETTINGS);
            }

        }

        public static StaticSettings.AttributeSet getAvailableSettings() {
            return StaticSettings.AttributeSet.of(ENABLED, ENTERPRISE_ENABLED, MAX_THREADS, THREAD_KEEP_ALIVE, THREAD_PRIO, ACTIVE_BY_DEFAULT,
                    WATCH_LOG_REFRESH_POLICY, WATCH_LOG_SYNC_INDEXING, WATCH_LOG_MAPPING_TOTAL_FIELDS_LIMIT, IndexNames.WATCHES,
                    IndexNames.WATCHES_STATE, IndexNames.WATCHES_TRIGGER_STATE, IndexNames.ACCOUNTS, IndexNames.LOG);
        }

        private final StaticSettings settings;
        private final IndexNames indexNames;

        public SignalsStaticSettings(Settings settings) {
            this.settings = new StaticSettings(settings, null);
            this.indexNames = new IndexNames(settings);
        }

        public boolean isEnabled() {
            return settings.get(ENABLED);
        }

        public int getMaxThreads() {
            return settings.get(MAX_THREADS);
        }

        public Duration getThreadKeepAlive() {
            return Duration.ofMillis(settings.get(THREAD_KEEP_ALIVE).millis());
        }

        public int getThreadPrio() {
            return settings.get(THREAD_PRIO);
        }

        public boolean isEnterpriseEnabled() {
            return settings.get(ENTERPRISE_ENABLED);
        }

        public IndexNames getIndexNames() {
            return indexNames;
        }

        public Settings getSettings() {
            return settings.getPlatformSettings();
        }

        public boolean isActiveByDefault() {
            return settings.get(ACTIVE_BY_DEFAULT);
        }

        public RefreshPolicy getWatchLogRefreshPolicy() {
            String value = settings.get(WATCH_LOG_REFRESH_POLICY);

            if (value == null) {
                return RefreshPolicy.NONE;
            } else if ("immediate".equalsIgnoreCase(value)) {
                return RefreshPolicy.IMMEDIATE;
            } else {
                return RefreshPolicy.NONE;
            }
        }

        public boolean isWatchLogSyncIndexingEnabled() {
            return settings.get(WATCH_LOG_SYNC_INDEXING);
        }

        public int getWatchLogMappingTotalFieldsLimit() {
            return settings.get(WATCH_LOG_MAPPING_TOTAL_FIELDS_LIMIT);
        }
    }

    public static interface ChangeListener {
        void onChange();
    }

}
