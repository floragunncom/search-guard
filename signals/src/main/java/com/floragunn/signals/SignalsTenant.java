package com.floragunn.signals;

import java.io.Closeable;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.quartz.Job;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchguard.internalauthtoken.InternalAuthTokenProvider;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.jobs.JobConfigListener;
import com.floragunn.searchsupport.jobs.SchedulerBuilder;
import com.floragunn.searchsupport.jobs.actions.SchedulerConfigUpdateAction;
import com.floragunn.searchsupport.jobs.config.JobDetailWithBaseConfig;
import com.floragunn.searchsupport.jobs.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.jobs.config.validation.ValidatingJsonParser;
import com.floragunn.signals.accounts.AccountRegistry;
import com.floragunn.signals.execution.ExecutionEnvironment;
import com.floragunn.signals.execution.SimulationMode;
import com.floragunn.signals.execution.WatchRunner;
import com.floragunn.signals.support.PrivilegedConfigClient;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.init.WatchInitializationService;
import com.floragunn.signals.watch.result.WatchLogIndexWriter;
import com.floragunn.signals.watch.result.WatchLogWriter;
import com.floragunn.signals.watch.state.WatchState;
import com.floragunn.signals.watch.state.WatchStateIndexReader;
import com.floragunn.signals.watch.state.WatchStateIndexWriter;
import com.floragunn.signals.watch.state.WatchStateManager;
import com.floragunn.signals.watch.state.WatchStateWriter;

public class SignalsTenant implements Closeable {
    private static final Logger log = LogManager.getLogger(SignalsTenant.class);

    public static SignalsTenant create(String name, Client client, ClusterService clusterService, ScriptService scriptService,
            NamedXContentRegistry xContentRegistry, NodeEnvironment nodeEnvironment, InternalAuthTokenProvider internalAuthTokenProvider,
            SignalsSettings settings, AccountRegistry accountRegistry) throws SchedulerException {
        SignalsTenant instance = new SignalsTenant(name, client, clusterService, scriptService, xContentRegistry, nodeEnvironment,
                internalAuthTokenProvider, settings, accountRegistry);

        instance.init();

        return instance;
    }

    private final SignalsSettings settings;
    private final String name;
    private final String scopedName;
    private final String configIndexName;
    private final String watchIdPrefix;
    private final Client privilegedConfigClient;
    private final Client client;
    private final ClusterService clusterService;
    private String nodeFilter;
    private final NamedXContentRegistry xContentRegistry;
    private final ScriptService scriptService;
    private final WatchLogWriter watchLogWriter;
    private final WatchStateManager watchStateManager;
    private final WatchStateWriter watchStateWriter;
    private final WatchStateIndexReader watchStateReader;
    private final InternalAuthTokenProvider internalAuthTokenProvider;
    private final AccountRegistry accountRegistry;
    private final String nodeName;
    private SignalsSettings.Tenant tenantSettings;

    private Scheduler scheduler;

    public SignalsTenant(String name, Client client, ClusterService clusterService, ScriptService scriptService,
            NamedXContentRegistry xContentRegistry, NodeEnvironment nodeEnvironment, InternalAuthTokenProvider internalAuthTokenProvider,
            SignalsSettings settings, AccountRegistry accountRegistry) {
        this.name = name;
        this.settings = settings;
        this.scopedName = "signals/" + name;
        this.configIndexName = settings.getStaticSettings().getIndexNames().getWatches();
        this.watchIdPrefix = name.replace("/", "\\/") + "/";
        this.client = client;
        this.privilegedConfigClient = new PrivilegedConfigClient(client);
        this.clusterService = clusterService;
        this.scriptService = scriptService;
        this.xContentRegistry = xContentRegistry;
        this.tenantSettings = settings.getTenant(name);
        this.nodeFilter = tenantSettings.getNodeFilter();
        this.watchLogWriter = WatchLogIndexWriter.forTenant(client, name, settings);
        this.watchStateManager = new WatchStateManager(name, clusterService.getNodeName());
        this.watchStateWriter = new WatchStateIndexWriter(watchIdPrefix, settings.getStaticSettings().getIndexNames().getWatchesState(),
                privilegedConfigClient);
        this.watchStateReader = new WatchStateIndexReader(name, watchIdPrefix, settings.getStaticSettings().getIndexNames().getWatchesState(),
                privilegedConfigClient);
        this.internalAuthTokenProvider = internalAuthTokenProvider;
        this.accountRegistry = accountRegistry;
        this.nodeName = clusterService.getNodeName();

        settings.addChangeListener(this.settingsChangeListener);
    }

    public void init() throws SchedulerException {
        log.info("Initializing alerting tenant " + name + "\nnodeFilter: " + nodeFilter);

        this.scheduler = new SchedulerBuilder<Watch>().client(privilegedConfigClient).name(scopedName)
                .configIndex(configIndexName, getActiveConfigQuery(name))
                .stateIndex(settings.getStaticSettings().getIndexNames().getWatchesTriggerState()).stateIndexIdPrefix(watchIdPrefix)
                .jobConfigFactory(new Watch.JobConfigFactory(name, watchIdPrefix, new WatchInitializationService(accountRegistry, scriptService)))
                .distributed(clusterService).jobFactory(jobFactory).nodeFilter(nodeFilter).jobConfigListener(jobConfigListener).build();

        if (this.tenantSettings.isActive()) {
            this.scheduler.start();
        }
    }

    public void pause() throws SchedulerException {
        log.info("Suspending scheduler of " + this);

        this.scheduler.standby();
    }

    public void resume() throws SchedulerException {
        if (this.scheduler.isShutdown()) {
            throw new IllegalStateException("Cannot resume scheduler which is shutdown: " + this.scheduler);
        }

        if (!this.scheduler.isStarted() || this.scheduler.isInStandbyMode()) {
            log.info("Resuming scheduler of " + this);

            this.scheduler.start();
        } else {
            log.info("Scheduler is already active " + this);
        }
    }

    public boolean isActive() throws SchedulerException {
        return this.scheduler != null && this.scheduler.isStarted() && !this.scheduler.isInStandbyMode();
    }

    public void shutdown() {
        try {
            this.scheduler.shutdown(true);
        } catch (SchedulerException e) {
            log.error("Error wile shutting down " + this, e);
        }
    }

    public synchronized void restart() throws SchedulerException {
        shutdown();
        init();
    }

    public void restartAsync() {
        new Thread() {
            public void run() {
                try {
                    restart();
                } catch (SchedulerException e) {
                    log.error("Error while restarting: " + SignalsTenant.this, e);
                }
            }
        }.start();
    }

    public boolean runsWatchLocally(String watchId) {
        try {
            return this.scheduler.getJobDetail(Watch.createJobKey(watchId)) != null;
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

    public int getLocalWatchCount() {
        try {
            // Note: The following call is synchronized on the job store, so use this call with care
            return this.scheduler.getJobKeys(GroupMatcher.anyJobGroup()).size();
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

    public String getWatchIdForConfigIndex(String watchId) {
        return watchIdPrefix + watchId;
    }

    public String getWatchIdForConfigIndex(Watch watch) {
        return getWatchIdForConfigIndex(watch.getId());
    }

    public IndexResponse addWatch(Watch watch, User user) throws IOException {

        try (XContentBuilder watchContentBuilder = XContentFactory.jsonBuilder()) {

            watch.setTenant(name);
            watch.getMeta().setLastEditByUser(user.getName());
            watch.getMeta().setLastEditByDate(new Date());
            watch.getMeta().setAuthToken(internalAuthTokenProvider.getJwt(user, watch.getIdAndHash()));

            watch.toXContent(watchContentBuilder, ToXContent.EMPTY_PARAMS);

            IndexResponse indexResponse = privilegedConfigClient.prepareIndex(getConfigIndexName(), null, getWatchIdForConfigIndex(watch.getId()))
                    .setSource(watchContentBuilder).setRefreshPolicy(RefreshPolicy.IMMEDIATE).execute().actionGet();

            if (indexResponse.getResult() == Result.CREATED) {
                watchStateWriter.put(watch.getId(), new WatchState(name));
            }

            if (indexResponse.getResult() == Result.CREATED || indexResponse.getResult() == Result.UPDATED) {
                SchedulerConfigUpdateAction.send(privilegedConfigClient, getScopedName());
            }

            return indexResponse;
        }
    }

    public IndexResponse addWatch(String watchId, String watchJsonString, User user) throws ConfigValidationException, IOException {

        ObjectNode watchJson = ValidatingJsonParser.readObject(watchJsonString);

        Watch watch = Watch.parse(new WatchInitializationService(accountRegistry, scriptService), getName(), watchId, watchJson, -1);

        watch.setTenant(name);
        watch.getMeta().setLastEditByUser(user.getName());
        watch.getMeta().setLastEditByDate(new Date());
        watch.getMeta().setAuthToken(internalAuthTokenProvider.getJwt(user, watch.getIdAndHash()));

        watchJson.put("_tenant", watch.getTenant());
        watchJson.set("_meta", watch.getMeta().toJsonNode());

        String newWatchJsonString = DefaultObjectMapper.writeJsonTree(watchJson);

        IndexResponse indexResponse = privilegedConfigClient.prepareIndex(getConfigIndexName(), null, getWatchIdForConfigIndex(watch.getId()))
                .setSource(newWatchJsonString, XContentType.JSON).setRefreshPolicy(RefreshPolicy.IMMEDIATE).execute().actionGet();

        if (indexResponse.getResult() == Result.CREATED) {
            watchStateWriter.put(watch.getId(), new WatchState(name));
        }

        if (indexResponse.getResult() == Result.CREATED || indexResponse.getResult() == Result.UPDATED) {
            SchedulerConfigUpdateAction.send(privilegedConfigClient, getScopedName());
        }
        return indexResponse;
    }

    public List<String> ack(String watchId, User user) {
        if (log.isInfoEnabled()) {
            log.info("ack(" + watchId + ", " + user + ")");
        }

        WatchState watchState = watchStateManager.getWatchState(watchId);

        List<String> result = watchState.ack(user != null ? user.getName() : null);

        watchStateWriter.put(watchId, watchState);

        return result;
    }

    public void ack(String watchId, String actionId, User user) {
        if (log.isInfoEnabled()) {
            log.info("ack(" + watchId + ", " + actionId + ", " + user + ")");
        }

        WatchState watchState = watchStateManager.getWatchState(watchId);

        watchState.getActionState(actionId).ack(user != null ? user.getName() : null);

        watchStateWriter.put(watchId, watchState);
    }

    public List<String> unack(String watchId, User user) {
        if (log.isInfoEnabled()) {
            log.info("unack(" + watchId + ", " + user + ")");
        }

        WatchState watchState = watchStateManager.getWatchState(watchId);

        List<String> result = watchState.unack(user != null ? user.getName() : null);

        watchStateWriter.put(watchId, watchState);

        return result;
    }

    public boolean unack(String watchId, String actionId, User user) {
        if (log.isInfoEnabled()) {
            log.info("unack(" + watchId + ", " + actionId + ", " + user + ")");
        }

        WatchState watchState = watchStateManager.getWatchState(watchId);

        boolean result = watchState.getActionState(actionId).unackIfPossible(user != null ? user.getName() : null);

        watchStateWriter.put(watchId, watchState);

        return result;
    }

    public WatchState getWatchState(String watchId) {
        return watchStateManager.getWatchState(watchId);
    }

    public void deleteTenantFromIndexes() {
        log.info("Deleting watches of " + this);

        SearchRequest searchRequest = new SearchRequest(this.configIndexName);
        searchRequest.source(new SearchSourceBuilder().query(getConfigQuery(this.name)).size(10000));
        // TODO scrolling

        SearchResponse searchResponse = this.privilegedConfigClient.search(searchRequest).actionGet();

        int seen = 0;
        int deletedWatches = 0;
        int deletedWatchStates = 0;

        for (SearchHit hit : searchResponse.getHits()) {
            seen++;

            DeleteResponse watchDeleteResponse = this.privilegedConfigClient.delete(new DeleteRequest(this.configIndexName, hit.getId())).actionGet();
            deletedWatches += watchDeleteResponse.getResult() == Result.DELETED ? 1 : 0;

            DeleteResponse watchStateDeleteResponse = this.privilegedConfigClient
                    .delete(new DeleteRequest(this.settings.getStaticSettings().getIndexNames().getWatchesState(), hit.getId())).actionGet();
            deletedWatchStates += watchStateDeleteResponse.getResult() == Result.DELETED ? 1 : 0;

            // TODO triggers
        }

        log.info("Deleted of  " + seen + ":\n" + deletedWatches + " watches\n" + deletedWatchStates + " watch states");
    }

    public void delete() {
        this.settings.removeChangeListener(this.settingsChangeListener);
        this.shutdown();
    }

    private final JobFactory jobFactory = new JobFactory() {

        @Override
        public Job newJob(TriggerFiredBundle bundle, Scheduler scheduler) throws SchedulerException {
            Watch watch = getConfig(bundle);
            WatchState watchState = watchStateManager.getWatchState(watch.getId());

            if (watchState.isRefreshBeforeExecuting()) {
                watchState = refreshState(watch, watchState);
            }

            return new WatchRunner(watch, client, accountRegistry, scriptService, watchLogWriter, watchStateWriter, watchState,
                    ExecutionEnvironment.SCHEDULED, SimulationMode.FOR_REAL, xContentRegistry, settings, nodeName, null, null);
        }

        private Watch getConfig(TriggerFiredBundle bundle) {
            return ((JobDetailWithBaseConfig) bundle.getJobDetail()).getBaseConfig(Watch.class);
        }

        private WatchState refreshState(Watch watch, WatchState state) {
            try {
                state = watchStateReader.get(watch.getId());
                state.setNode(nodeName);
                return state;
            } catch (Exception e) {
                log.error("Error while refreshing WatchState of " + watch.getId() + ";\nUsing original state", e);
                return state;
            }
        }

    };

    private final JobConfigListener<Watch> jobConfigListener = new JobConfigListener<Watch>() {

        @Override
        public void onChange(Set<Watch> newJobs, Map<Watch, Watch> updatedJobs, Set<Watch> deletedJobs) {
            for (Watch deletedWatch : deletedJobs) {
                watchStateManager.delete(deletedWatch.getId());
            }
        }

        @Override
        public void onInit(Set<Watch> watches) {
            Set<String> watchIds = watches.stream().map((watch) -> watch.getId()).collect(Collectors.toSet());
            Map<String, WatchState> dirtyStates = watchStateManager.reset(watchStateReader.get(watchIds));

            if (!dirtyStates.isEmpty()) {
                watchStateWriter.putAll(dirtyStates);
            }
        }

    };

    public WatchStateManager getWatchStateManager() {
        return watchStateManager;
    }

    private QueryBuilder getActiveConfigQuery(String tenant) {
        return QueryBuilders.boolQuery().must(getConfigQuery(tenant)).mustNot(QueryBuilders.termQuery("active", false));
    }

    private QueryBuilder getConfigQuery(String tenant) {
        return QueryBuilders.boolQuery().must(QueryBuilders.termQuery("_tenant", tenant));
    }

    public String getName() {
        return name;
    }

    public String getConfigIndexName() {
        return configIndexName;
    }

    public String getScopedName() {
        return scopedName;
    }

    @Override
    public void close() throws IOException {
        this.shutdown();
    }

    private final SignalsSettings.ChangeListener settingsChangeListener = new SignalsSettings.ChangeListener() {

        @Override
        public void onChange() {

            if (scheduler == null) {
                // We have not been initialized yet. Do nothing.
                return;
            }

            try {
                tenantSettings = settings.getTenant(name);

                if (!Objects.equals(nodeFilter, tenantSettings.getNodeFilter())) {
                    log.info("Restarting tenant " + name + " because node filter has changed: " + nodeFilter + " <> "
                            + tenantSettings.getNodeFilter());
                    nodeFilter = tenantSettings.getNodeFilter();
                    restartAsync();
                }

                boolean active = tenantSettings.isActive() && settings.getDynamicSettings().isActive();

                if (active != isActive()) {
                    if (active) {
                        resume();
                    } else {
                        pause();
                    }
                }
            } catch (SchedulerException e) {
                log.error("Error in " + this, e);
            }

        }
    };

    @Override
    public String toString() {
        return "SignalsTenant " + name;
    }

    public WatchStateIndexReader getWatchStateReader() {
        return watchStateReader;
    }
}
