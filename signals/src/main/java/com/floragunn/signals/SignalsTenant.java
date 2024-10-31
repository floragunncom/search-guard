/*
 * Copyright 2019-2022 floragunn GmbH
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

package com.floragunn.signals;

import java.io.Closeable;
import java.io.IOException;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.floragunn.searchsupport.jobs.config.schedule.DefaultScheduleFactory;
import com.floragunn.signals.proxy.service.HttpProxyHostRegistry;
import com.floragunn.signals.truststore.service.TrustManagerRegistry;
import com.floragunn.signals.watch.common.Ack;
import com.floragunn.signals.watch.common.throttle.DefaultThrottlePeriodParser;
import com.floragunn.signals.watch.common.throttle.ValidatingThrottlePeriodParser;
import com.floragunn.signals.watch.common.ValidationLevel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.quartz.Job;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.DocWriter;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.internalauthtoken.InternalAuthTokenProvider;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentState.State;
import com.floragunn.searchsupport.diag.DiagnosticContext;
import com.floragunn.searchsupport.jobs.JobConfigListener;
import com.floragunn.searchsupport.jobs.SchedulerBuilder;
import com.floragunn.searchsupport.jobs.actions.SchedulerConfigUpdateAction;
import com.floragunn.searchsupport.jobs.config.JobDetailWithBaseConfig;
import com.floragunn.signals.accounts.AccountRegistry;
import com.floragunn.signals.execution.ExecutionEnvironment;
import com.floragunn.signals.execution.SimulationMode;
import com.floragunn.signals.execution.WatchRunner;
import com.floragunn.signals.settings.SignalsSettings;
import com.floragunn.signals.support.ToXParams;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.action.invokers.AlertAction;
import com.floragunn.signals.watch.checks.StaticInput;
import com.floragunn.signals.watch.init.WatchInitializationService;
import com.floragunn.signals.watch.result.WatchLog;
import com.floragunn.signals.watch.result.WatchLogIndexWriter;
import com.floragunn.signals.watch.result.WatchLogWriter;
import com.floragunn.signals.watch.state.WatchState;
import com.floragunn.signals.watch.state.WatchStateIndexReader;
import com.floragunn.signals.watch.state.WatchStateIndexWriter;
import com.floragunn.signals.watch.state.WatchStateManager;

import static com.floragunn.signals.watch.common.ValidationLevel.LENIENT;

public class SignalsTenant implements Closeable {
    private static final Logger log = LogManager.getLogger(SignalsTenant.class);

    public static SignalsTenant create(String name, Client client, ClusterService clusterService, NodeEnvironment nodeEnvironment,
            ScriptService scriptService, NamedXContentRegistry xContentRegistry, InternalAuthTokenProvider internalAuthTokenProvider,
            SignalsSettings settings, AccountRegistry accountRegistry, ComponentState tenantState, DiagnosticContext diagnosticContext,
            ThreadPool threadPool, TrustManagerRegistry trustManagerRegistry, HttpProxyHostRegistry httpProxyHostRegistry,
                                       FeatureService featureService, DefaultScheduleFactory signalsScheduleFactory)
            throws SchedulerException {
        SignalsTenant instance = new SignalsTenant(name, client, clusterService, nodeEnvironment, scriptService, xContentRegistry,
                internalAuthTokenProvider, settings, accountRegistry, tenantState, diagnosticContext, threadPool,
                trustManagerRegistry, httpProxyHostRegistry, featureService, signalsScheduleFactory);

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
    private final NodeEnvironment nodeEnvironment;
    private String nodeFilter;
    private final NamedXContentRegistry xContentRegistry;
    private final ScriptService scriptService;
    private final WatchStateManager watchStateManager;
    private final WatchStateIndexWriter watchStateWriter;
    private final WatchStateIndexReader watchStateReader;
    private final InternalAuthTokenProvider internalAuthTokenProvider;
    private final AccountRegistry accountRegistry;
    private final String nodeName;
    private final ComponentState tenantState;
    private SignalsSettings.Tenant tenantSettings;
    private final DiagnosticContext diagnosticContext;
    private Scheduler scheduler;

    private final TrustManagerRegistry trustManagerRegistry;
    private final HttpProxyHostRegistry httpProxyHostRegistry;
    private final FeatureService featureService;
    private final DefaultScheduleFactory signalsScheduleFactory;

    public SignalsTenant(String name, Client client, ClusterService clusterService, NodeEnvironment nodeEnvironment, ScriptService scriptService,
            NamedXContentRegistry xContentRegistry, InternalAuthTokenProvider internalAuthTokenProvider, SignalsSettings settings,
            AccountRegistry accountRegistry, ComponentState tenantState, DiagnosticContext diagnosticContext, ThreadPool threadPool,
        TrustManagerRegistry trustManagerRegistry, HttpProxyHostRegistry httpProxyHostRegistry, FeatureService featureService,
                         DefaultScheduleFactory signalsScheduleFactory) {
        this.name = name;
        this.settings = settings;
        this.scopedName = "signals/" + name;
        this.configIndexName = settings.getStaticSettings().getIndexNames().getWatches();
        this.watchIdPrefix = name.replace("/", "\\/") + "/";
        this.client = client;
        this.privilegedConfigClient = PrivilegedConfigClient.adapt(client);
        this.clusterService = clusterService;
        this.nodeEnvironment = nodeEnvironment;
        this.scriptService = scriptService;
        this.xContentRegistry = xContentRegistry;
        this.tenantSettings = settings.getTenant(name);
        this.nodeFilter = tenantSettings.getNodeFilter();
        this.watchStateManager = new WatchStateManager(name, clusterService.getNodeName());
        this.watchStateWriter = new WatchStateIndexWriter(watchIdPrefix, settings.getStaticSettings().getIndexNames().getWatchesState(),
                privilegedConfigClient);
        this.watchStateReader = new WatchStateIndexReader(name, watchIdPrefix, settings.getStaticSettings().getIndexNames().getWatchesState(),
                privilegedConfigClient);
        this.internalAuthTokenProvider = internalAuthTokenProvider;
        this.accountRegistry = accountRegistry;
        this.nodeName = clusterService.getNodeName();
        this.tenantState = tenantState;
        this.diagnosticContext = diagnosticContext;
        this.trustManagerRegistry = Objects.requireNonNull(trustManagerRegistry, "Trust manager registry is required");
        this.httpProxyHostRegistry = Objects.requireNonNull(httpProxyHostRegistry, "Http proxy host registry is required");
        this.featureService = Objects.requireNonNull(featureService, "Feature service is required");
        this.signalsScheduleFactory = Objects.requireNonNull(signalsScheduleFactory, "Schedule factory is required");
        settings.addChangeListener(this.settingsChangeListener);
    }

    public SignalsTenant(String name, Client client, ClusterService clusterService, NodeEnvironment nodeEnvironment, ScriptService scriptService,
            NamedXContentRegistry xContentRegistry, InternalAuthTokenProvider internalAuthTokenProvider, SignalsSettings settings,
            AccountRegistry accountRegistry, DiagnosticContext diagnosticContext, ThreadPool threadPool,
            TrustManagerRegistry trustManagerRegistry, HttpProxyHostRegistry httpProxyHostRegistry, FeatureService featureService,
                         DefaultScheduleFactory signalsScheduleFactory) {
        this(name, client, clusterService, nodeEnvironment, scriptService, xContentRegistry, internalAuthTokenProvider, settings, accountRegistry,
                new ComponentState(0, null, "tenant"), diagnosticContext, threadPool, trustManagerRegistry, httpProxyHostRegistry,
                featureService, signalsScheduleFactory
        );
    }

    public void init() throws SchedulerException {
        if (this.tenantSettings.isActive()) {
            doInit();
        } else {
            this.tenantState.setState(State.SUSPENDED);
        }
    }

    private void doInit() throws SchedulerException {
        log.info("Initializing alerting tenant " + name + "\nnodeFilter: " + nodeFilter);
        tenantState.setState(ComponentState.State.INITIALIZING);

        WatchInitializationService initContext = new WatchInitializationService(accountRegistry, scriptService, trustManagerRegistry,
                httpProxyHostRegistry, new DefaultThrottlePeriodParser(settings), signalsScheduleFactory, LENIENT);
        this.scheduler = new SchedulerBuilder<Watch>()//
                .client(privilegedConfigClient)//
                .name(scopedName)//
                .configIndex(configIndexName, getActiveConfigQuery(name))//
                .stateIndex(settings.getStaticSettings().getIndexNames().getWatchesTriggerState())//
                .stateIndexIdPrefix(watchIdPrefix)//
                .jobConfigFactory(new Watch.JobConfigFactory(name, watchIdPrefix, initContext))//
                .distributed(clusterService, nodeEnvironment)//
                .jobFactory(jobFactory)//
                .nodeFilter(nodeFilter)//
                .jobConfigListener(jobConfigListener)//
                .maxThreads(settings.getStaticSettings().getMaxThreads())//
                .threadKeepAlive(settings.getStaticSettings().getThreadKeepAlive())//
                .threadPriority(settings.getStaticSettings().getThreadPrio())//
                .build();
        this.scheduler.start();
    }

    public void pause() throws SchedulerException {
        log.info("Suspending scheduler of " + this);

        if (this.scheduler != null) {
            this.tenantState.setState(State.SUSPENDED);
            this.scheduler.standby();
        }
    }

    public void resume() throws SchedulerException {
        if (this.scheduler == null || this.scheduler.isShutdown()) {
            doInit();
        } else if (!this.scheduler.isStarted() || this.scheduler.isInStandbyMode()) {
            log.info("Resuming scheduler of " + this);
            this.tenantState.setState(State.INITIALIZED);
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
            if (this.scheduler != null) {
                if (log.isDebugEnabled()) {
                    log.debug("Going to shutdown " + this.scheduler);
                }

                this.scheduler.shutdown(true);
                tenantState.setState(ComponentState.State.DISABLED);
            }
        } catch (SchedulerException e) {
            log.error("Error wile shutting down " + this, e);
        }
    }

    public void shutdownHard() {
        try {
            if (this.scheduler != null) {
                if (log.isDebugEnabled()) {
                    log.debug("Going to shutdown " + this.scheduler);
                }

                this.scheduler.shutdown(false);
                tenantState.setState(ComponentState.State.DISABLED);
                this.scheduler = null;
            }
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
            return this.scheduler != null && this.scheduler.getJobDetail(Watch.createJobKey(watchId)) != null;
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

    public Watch getLocallyRunningWatch(String watchId) {
        if (this.scheduler == null) {
            return null;
        }

        try {
            JobDetailWithBaseConfig jobDetail = (JobDetailWithBaseConfig) this.scheduler.getJobDetail(Watch.createJobKey(watchId));
            
            if (jobDetail == null) {
                return null;
            }
            
            return jobDetail.getBaseConfig(Watch.class);
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

    public int getLocalWatchCount() {
        try {
            if (this.scheduler == null) {
                return 0;
            }

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

    public DocWriteResponse addWatch(Watch watch, User user, ValidationLevel lifeCycleStage) throws IOException {

        try {
            return addWatch(watch.getId(), Strings.toString(watch), user, lifeCycleStage);
        } catch (ConfigValidationException e) {
            // This should not happen
            throw new RuntimeException(e);
        }
    }

    public DocWriteResponse addWatch(String watchId, String watchJsonString, User user, ValidationLevel validationLevel)
        throws ConfigValidationException, IOException {

        if (log.isInfoEnabled()) {
            log.info("addWatch(" + watchId + ") on " + this);
        }

        Map<String, Object> watchJson = new LinkedHashMap<>(DocReader.json().readObject(watchJsonString));

        WatchInitializationService initializationService = new WatchInitializationService(accountRegistry, scriptService,//
            trustManagerRegistry, httpProxyHostRegistry, new ValidatingThrottlePeriodParser(settings), signalsScheduleFactory, validationLevel);
        Watch watch = Watch.parse(initializationService, getName(), watchId, DocNode.wrap(watchJson), -1);

        watch.setTenant(name);
        watch.getMeta().setLastEditByUser(user.getName());
        watch.getMeta().setLastEditByDate(new Date());
        watch.getMeta().setAuthToken(internalAuthTokenProvider.getJwt(user, watch.getIdAndHash()));

        watchJson.put("_tenant", watch.getTenant());
        watchJson.put("_meta", watch.getMeta().toMap());
        watchJson.put("_name", watchId);
        
        StaticInput.patchForIndexMappingBugFix(watchJson);

        String newWatchJsonString = DocWriter.json().writeAsString(watchJson);

        DocWriteResponse indexResponse = privilegedConfigClient.prepareIndex().setIndex(getConfigIndexName()).setId(getWatchIdForConfigIndex(watch.getId()))
                .setSource(newWatchJsonString, XContentType.JSON).setRefreshPolicy(RefreshPolicy.IMMEDIATE).execute().actionGet();

        if (log.isDebugEnabled()) {
            log.debug("IndexResponse from addWatch()\n" + Strings.toString(indexResponse));
        }

        if (indexResponse.getResult() == Result.CREATED) {
            watchStateWriter.put(watch.getId(), new WatchState(name), new ActionListener<DocWriteResponse>() {

                @Override
                public void onResponse(DocWriteResponse response) {
                    SchedulerConfigUpdateAction.send(privilegedConfigClient, getScopedName());
                }

                @Override
                public void onFailure(Exception e) {
                    log.warn("Error while writing initial state for " + watch + ". Ignoring", e);
                    SchedulerConfigUpdateAction.send(privilegedConfigClient, getScopedName());
                }

            });
        } else if (indexResponse.getResult() == Result.UPDATED) {
            SchedulerConfigUpdateAction.send(privilegedConfigClient, getScopedName());
        }

        return indexResponse;
    }

    public Map<String, Ack> ack(String watchId, User user) throws NoSuchWatchOnThisNodeException {
        if (log.isInfoEnabled()) {
            log.info("ack(" + watchId + ", " + user + ")");
        }
        
        Watch watch = getLocallyRunningWatch(watchId);
        
        if (watch == null) {
            throw new NoSuchWatchOnThisNodeException(watchId, nodeName);
        }

        WatchState watchState = watchStateManager.getWatchState(watchId);

        Map<String, Ack> result = watchState.ack(user != null ? user.getName() : null, watch);

        watchStateWriter.put(watchId, watchState);

        return result;
    }

    public WatchState ack(String watchId, String actionId, User user) throws NoSuchWatchOnThisNodeException, NoSuchActionException, NotAcknowledgeableException {
        if (log.isInfoEnabled()) {
            log.info("ack(" + watchId + ", " + actionId + ", " + user + ")");
        }

        Watch watch = getLocallyRunningWatch(watchId);
        
        if (watch == null) {
            throw new NoSuchWatchOnThisNodeException(watchId, nodeName);
        }
        
        AlertAction action = watch.getActionByName(actionId);
        
        if (!action.isAckEnabled()) {
            throw new NotAcknowledgeableException(watchId, actionId);
        }
        
        WatchState watchState = watchStateManager.getWatchState(watchId);

        watchState.getActionState(actionId).ack(user != null ? user.getName() : null);

        watchStateWriter.put(watchId, watchState);
        return watchState;
    }

    public List<String> unack(String watchId, User user) throws NoSuchWatchOnThisNodeException {
        if (log.isInfoEnabled()) {
            log.info("unack(" + watchId + ", " + user + ")");
        }

        WatchState watchState = watchStateManager.peekWatchState(watchId);

        if (watchState == null) {
            throw new NoSuchWatchOnThisNodeException(watchId, nodeName);
        }

        List<String> result = watchState.unack(user != null ? user.getName() : null);

        if (log.isDebugEnabled()) {
            log.debug("Unacked actions: " + result);
        }

        watchStateWriter.put(watchId, watchState);

        return result;
    }

    public boolean unack(String watchId, String actionId, User user) throws NoSuchWatchOnThisNodeException {
        if (log.isInfoEnabled()) {
            log.info("unack(" + watchId + ", " + actionId + ", " + user + ")");
        }

        WatchState watchState = watchStateManager.peekWatchState(watchId);

        if (watchState == null) {
            throw new NoSuchWatchOnThisNodeException(watchId, nodeName);
        }

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

        SearchResponse searchResponse = this.privilegedConfigClient.search(searchRequest).actionGet();
        try {

            int seen = 0;
            int deletedWatches = 0;
            int deletedWatchStates = 0;

            for (SearchHit hit : searchResponse.getHits()) {
                seen++;

                DeleteResponse
                    watchDeleteResponse =
                    this.privilegedConfigClient.delete(new DeleteRequest(this.configIndexName, hit.getId())).actionGet();
                deletedWatches += watchDeleteResponse.getResult() == Result.DELETED ? 1 : 0;

                DeleteResponse
                    watchStateDeleteResponse =
                    this.privilegedConfigClient.delete(new DeleteRequest(this.settings.getStaticSettings()
                        .getIndexNames()
                        .getWatchesState(), hit.getId())).actionGet();
                deletedWatchStates += watchStateDeleteResponse.getResult() == Result.DELETED ? 1 : 0;

                // TODO triggers
            }

            log.info("Deleted of  " + seen + ":\n" + deletedWatches + " watches\n" + deletedWatchStates + " watch states");
        } finally {
            searchResponse.decRef();
        }
    }

    public void delete() {
        this.settings.removeChangeListener(this.settingsChangeListener);
        this.shutdown();
    }

    private final JobFactory jobFactory = new JobFactory() {

        @Override
        public Job newJob(TriggerFiredBundle bundle, Scheduler scheduler) throws SchedulerException {
            Watch watch = getConfig(bundle);

            if (log.isDebugEnabled()) {
                log.debug("newJob() on " + SignalsTenant.this + "@" + SignalsTenant.this.hashCode() + ": " + watch);
            }

            WatchState watchState = watchStateManager.getWatchState(watch.getId());

            if (watchState.isRefreshBeforeExecuting()) {
                watchState = refreshState(watch, watchState);
            }

            WatchLogWriter watchLogWriter = WatchLogIndexWriter.forTenant(client, name, settings,
                    ToXParams.of(WatchLog.ToXContentParams.INCLUDE_DATA, watch.isLogRuntimeData()));

            return new WatchRunner(watch, client, accountRegistry, scriptService, watchLogWriter, watchStateWriter, diagnosticContext, watchState,
                    ExecutionEnvironment.SCHEDULED, SimulationMode.FOR_REAL, xContentRegistry, settings, nodeName, null, null,
                    trustManagerRegistry, clusterService, featureService);
        }

        private Watch getConfig(TriggerFiredBundle bundle) {
            return ((JobDetailWithBaseConfig) bundle.getJobDetail()).getBaseConfig(Watch.class);
        }

        private WatchState refreshState(Watch watch, WatchState oldState) {
            try {
                if (log.isDebugEnabled()) {
                    log.debug("Refreshing state for " + watch.getId() + "\nOld state: " + (oldState != null ? Strings.toString(oldState) : null));
                }
                WatchState newState = watchStateReader.get(watch.getId());

                if (newState.getNode() == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("Got refreshed state for " + watch.getId()
                                + "\nThis however has a null node. Thus, it is probably the initial default state. Discarding: "
                                + (oldState != null ? Strings.toString(oldState) : null));
                    }

                    return oldState;
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Refreshed state for " + watch.getId() + "\nNew state: " + (oldState != null ? Strings.toString(oldState) : null));
                    }

                    newState.setNode(nodeName);
                    return newState;
                }

            } catch (Exception e) {
                log.error("Error while refreshing WatchState of " + watch.getId() + ";\nUsing original state", e);
                return oldState;
            }
        }

    };

    private final JobConfigListener<Watch> jobConfigListener = new JobConfigListener<Watch>() {

        @Override
        public void onInit(Set<Watch> watches) {
            Set<String> watchIds = watches.stream().map((watch) -> watch.getId()).collect(Collectors.toSet());

            tenantState.setState(State.INITIALIZING, "reading_states");

            Map<String, WatchState> dirtyStates = watchStateManager.reset(watchStateReader.get(watchIds), watchIds);

            if (!dirtyStates.isEmpty()) {
                tenantState.setState(State.INITIALIZING, "writing_states");

                watchStateWriter.putAll(dirtyStates);
            }

            tenantState.setState(State.INITIALIZED);
        }

        @Override
        public void beforeChange(Set<Watch> newJobs) {
            if (newJobs != null && newJobs.size() > 0) {
                Set<String> watchIds = newJobs.stream().map((watch) -> watch.getId()).collect(Collectors.toSet());

                tenantState.setState(State.INITIALIZING, "reading_states");

                if (log.isDebugEnabled()) {
                    log.debug("Reading states of newly arrived watches from index: " + watchIds);
                }

                Map<String, WatchState> statesFromIndex = watchStateReader.get(watchIds);

                Map<String, WatchState> dirtyStates = watchStateManager.add(statesFromIndex, watchIds);

                if (!dirtyStates.isEmpty()) {
                    tenantState.setState(State.INITIALIZING, "writing_states");

                    if (log.isDebugEnabled()) {
                        log.debug("Updating dirty states: " + dirtyStates);
                    }

                    watchStateWriter.putAll(dirtyStates);
                }

                tenantState.setState(State.INITIALIZED);
            }
        }

        @Override
        public void afterChange(Set<Watch> newJobs, Map<Watch, Watch> updatedJobs, Set<Watch> deletedJobs) {
            for (Watch deletedWatch : deletedJobs) {
                watchStateManager.delete(deletedWatch.getId());
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

    public SignalsSettings getSettings() {
        return settings;
    }
}
