package com.floragunn.signals;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

import com.floragunn.searchsupport.util.duration.ConstantDurationExpression;
import com.floragunn.searchsupport.util.duration.DurationExpression;
import com.floragunn.signals.actions.settings.update.SettingsUpdateAction;
import com.floragunn.signals.support.LuckySisyphos;
import com.floragunn.signals.support.PrivilegedConfigClient;

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

    public void refresh(Client client) {
        this.dynamicSettings.refresh(client);
    }

    public void addChangeListener(ChangeListener changeListener) {
        this.dynamicSettings.addChangeListener(changeListener);
    }

    public void removeChangeListener(ChangeListener changeListener) {
        this.dynamicSettings.removeChangeListener(changeListener);
    }

    public void update(Client client, String key, String value) {
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

        private final String indexName;
        private final StaticSettings staticSettings;

        private volatile Settings settings = Settings.builder().build();
        private volatile DurationExpression defaultThrottlePeriod;

        private Collection<ChangeListener> changeListeners = Collections.newSetFromMap(new ConcurrentHashMap<ChangeListener, Boolean>());

        DynamicSettings(String indexName, StaticSettings staticSettings) {
            this.indexName = indexName;
            this.staticSettings = staticSettings;
        }

        boolean isActive() {
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
            return new Tenant(settings.getAsSettings("tenant." + name));
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

        public void update(Client client, String key, String value) {
            PrivilegedConfigClient.adapt(client)
                    .index(new IndexRequest(indexName).id(key).source("value", value).setRefreshPolicy(RefreshPolicy.IMMEDIATE)).actionGet();

            SettingsUpdateAction.send(client);
        }

        public void update(Client client, String key, String value, String... moreKeyValue) {
            PrivilegedConfigClient.adapt(client)
                    .index(new IndexRequest(indexName).id(key).source("value", value).setRefreshPolicy(RefreshPolicy.IMMEDIATE)).actionGet();

            if (moreKeyValue != null) {
                for (int i = 0; i < moreKeyValue.length; i += 2) {
                    PrivilegedConfigClient.adapt(client).index(new IndexRequest(indexName).id(moreKeyValue[i]).source("value", moreKeyValue[i + 1])
                            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)).actionGet();
                }
            }

            SettingsUpdateAction.send(client);
        }

        void refresh(Client client) {
            try {
                SearchResponse response = LuckySisyphos.tryHard(() -> PrivilegedConfigClient.adapt(client)
                        .search(new SearchRequest(indexName).source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(1000)))
                        .actionGet());

                Settings.Builder newDynamicSettings = Settings.builder();

                for (SearchHit hit : response.getHits()) {
                    if (hit.getSourceAsMap().get("value") != null) {
                        newDynamicSettings.put(hit.getId(), hit.getSourceAsMap().get("value").toString());
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
                throw new RuntimeException("Error while loading settings", e);
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
                    INTERNAL_AUTH_TOKEN_SIGNING_KEY, INTERNAL_AUTH_TOKEN_ENCRYPTION_KEY, WATCH_LOG_INDEX);
        }

        public static Setting<?> getSetting(String key) {
            for (Setting<?> setting : getAvailableSettings()) {
                if (setting.match(key)) {
                    return setting;
                }
            }

            return null;
        }
       
        public Settings getSettings() {
            return settings;
        }

    }

    public static class Tenant {
        static final Setting<String> NODE_FILTER = Setting.simpleString("node_filter");
        static final Setting<Boolean> ACTIVE = Setting.boolSetting("active", Boolean.TRUE);

        private final Settings settings;

        Tenant(Settings settings) {
            this.settings = settings;
        }

        public String getNodeFilter() {
            String result = NODE_FILTER.get(settings);

            if (result.isEmpty()) {
                return null;
            } else {
                return result;
            }
        }

        public boolean isActive() {
            return ACTIVE.get(settings);
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
            return Arrays.asList(ENABLED, ENTERPRISE_ENABLED, IndexNames.WATCHES, IndexNames.WATCHES_STATE, IndexNames.WATCHES_TRIGGER_STATE, IndexNames.ACCOUNTS,
                    IndexNames.LOG);
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
