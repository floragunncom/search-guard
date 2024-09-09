package com.floragunn.aim.policy.instance;

import com.floragunn.aim.AutomatedIndexManagementSettings;
import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.PolicyService;
import com.floragunn.aim.policy.actions.Action;
import com.floragunn.aim.policy.conditions.Condition;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.ThreadPool;

import java.security.SecureRandom;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

//Runs only on master
public final class PolicyInstanceHandler {
    private static final Logger LOG = LogManager.getLogger(PolicyInstanceHandler.class);

    private final Client client;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final AutomatedIndexManagementSettings settings;
    private final PolicyInstanceService policyInstanceService;
    private final PolicyService policyService;
    private final ScheduledExecutorService scheduler;
    private final Map<String, Map.Entry<ScheduledFuture<?>, PolicyInstance>> scheduledPolicyInstances;
    private final ClusterStateListener clusterStateListener;
    private final AutomatedIndexManagementSettings.Dynamic.ChangeListener settingsChangeListener;
    private final Condition.Factory conditionFactory;
    private final Action.Factory actionFactory;
    private final PolicyInstance.ExecutionContext executionContext;

    private volatile boolean initialized = false;
    private PolicyInstanceStateLogHandler policyInstanceStateLogHandler;

    public PolicyInstanceHandler(AutomatedIndexManagementSettings settings, PolicyService policyService, PolicyInstanceService policyInstanceService,
            Client client, ThreadPool threadPool, ClusterService clusterService, Condition.Factory conditionFactory, Action.Factory actionFactory) {
        this.client = client;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.settings = settings;
        this.conditionFactory = conditionFactory;
        this.actionFactory = actionFactory;
        this.policyInstanceService = policyInstanceService;
        this.policyService = policyService;

        ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(settings.getStatic().getThreadPoolSize());
        scheduler.setRemoveOnCancelPolicy(true);
        scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        this.scheduler = scheduler;
        scheduledPolicyInstances = new HashMap<>();
        clusterStateListener = clusterChangedEvent -> {
            if (!clusterChangedEvent.metadataChanged()) {
                return;
            }
            List<String> deletedIndices = new ArrayList<>();
            Map<String, String> createdIndices = new HashMap<>();
            for (Index index : clusterChangedEvent.indicesDeleted()) {
                IndexMetadata indexMetadata = clusterChangedEvent.previousState().metadata().index(index);
                String policyName = settings.getStatic().getPolicyName(indexMetadata.getSettings());
                if (!Strings.isNullOrEmpty(policyName)) {
                    deletedIndices.add(index.getName());
                }
            }
            for (Map.Entry<String, IndexMetadata> index : clusterChangedEvent.state().metadata().indices().entrySet()) {
                String policyName = settings.getStatic().getPolicyName(index.getValue().getSettings());
                if (!Strings.isNullOrEmpty(policyName)) {
                    LOG.trace("New index '{}' with settings:\n{}", index.getKey(), Strings.toString(index.getValue().getSettings(), true, true));
                    createdIndices.put(index.getKey(), policyName);
                }
            }
            threadPool.generic().submit(() -> handleInstanceDeleteCreate(deletedIndices, createdIndices));
        };
        settingsChangeListener = changed -> {
            if (changed.contains(AutomatedIndexManagementSettings.Dynamic.EXECUTION_PERIOD)) {
                synchronized (this) {
                    for (String index : scheduledPolicyInstances.keySet()) {
                        ScheduledFuture<?> instance = scheduledPolicyInstances.remove(index).getKey();
                        instance.cancel(false);
                        if (!instance.isCancelled() && !instance.isDone()) {
                            LOG.debug("Could not cancel job for index '{}'", index);
                        }
                    }
                }
                threadPool.generic().submit(PolicyInstanceHandler.this::checkAllIndices);
            }
            if (changed.contains(AutomatedIndexManagementSettings.Dynamic.STATE_LOG_ACTIVE)) {
                if (settings.getDynamic().getStateLogActive()) {
                    threadPool.generic().submit(() -> initStateLogHandler(() -> {
                    }));
                } else {
                    stopStateLogHandler();
                }
            }
        };
        executionContext = new PolicyInstance.ExecutionContext(clusterService, client, settings, this.policyInstanceService);
    }

    public synchronized void init() {
        if (!initialized) {
            threadPool.generic().submit(() -> initStateLogHandler(() -> {
                threadPool.generic().submit(this::checkAllIndices);
                clusterService.addListener(clusterStateListener);
                settings.getDynamic().addChangeListener(settingsChangeListener);
                initialized = true;
            }));
        }
    }

    private synchronized void initStateLogHandler(Runnable onDone) {
        if (settings.getStatic().stateLog().isEnabled()) {
            LOG.info("Starting state log");
            policyInstanceStateLogHandler = new PolicyInstanceStateLogHandler(settings, client, policyService, policyInstanceService,
                    conditionFactory, actionFactory);
            policyInstanceStateLogHandler.init(new PolicyInstanceStateLogHandler.StateLogReadyListener() {
                @Override
                public void onLogReady() {
                    LOG.debug("State log started");
                    onDone.run();
                }

                @Override
                public void onLogFailure(Exception e) {
                    LOG.error("Failed to initialize policy instance state log handler", e);
                    onDone.run();
                }
            });
        } else {
            onDone.run();
        }
    }

    public synchronized void stop() {
        clusterService.removeListener(clusterStateListener);
        settings.getDynamic().removeChangeListener(settingsChangeListener);
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(1, TimeUnit.MINUTES)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
        }
        scheduledPolicyInstances.clear();
        stopStateLogHandler();
        initialized = false;
    }

    private synchronized void stopStateLogHandler() {
        if (policyInstanceStateLogHandler != null) {
            LOG.info("Stopping state log");
            policyInstanceStateLogHandler.stop();
            policyInstanceStateLogHandler = null;
        }
    }

    private synchronized void checkAllIndices() {
        Map<String, String> created = new HashMap<>();
        List<String> deleted = new ArrayList<>();
        for (Map.Entry<String, IndexMetadata> index : clusterService.state().metadata().indices().entrySet()) {
            String policyName = settings.getStatic().getPolicyName(index.getValue().getSettings());
            if (!Strings.isNullOrEmpty(policyName)) {
                created.put(index.getKey(), policyName);
            } else if (policyInstanceExistsForIndex(index.getKey())) {
                deleted.add(index.getKey());
            }
        }
        handleInstanceDeleteCreate(deleted, created);
    }

    public void handlePoliciesCreate(List<String> policyNames) {
        if (!initialized) {
            return;
        }
        Map<String, String> indices = new HashMap<>();
        for (Map.Entry<String, IndexMetadata> index : clusterService.state().metadata().indices().entrySet()) {
            String currentPolicyName = settings.getStatic().getPolicyName(index.getValue().getSettings());
            if (currentPolicyName != null && policyNames.contains(currentPolicyName)) {
                indices.put(index.getKey(), currentPolicyName);
            }
        }
        if (!indices.isEmpty()) {
            threadPool.generic().submit(() -> handleInstanceDeleteCreate(ImmutableList.empty(), indices));
        }
    }

    public void handlePoliciesDelete(List<String> policyNames) {
        if (!initialized) {
            return;
        }
        List<String> indices = new ArrayList<>();
        for (Map.Entry<String, IndexMetadata> index : clusterService.state().metadata().indices().entrySet()) {
            String currentPolicyName = settings.getStatic().getPolicyName(index.getValue().getSettings());
            if (currentPolicyName != null && policyNames.contains(currentPolicyName)) {
                indices.add(index.getKey());
            }
        }
        if (!indices.isEmpty()) {
            threadPool.generic().submit(() -> handleInstanceDeleteCreate(indices, ImmutableMap.empty()));
        }
    }

    private synchronized void handleInstanceDeleteCreate(List<String> deleted, Map<String, String> created) {
        if (deleted.isEmpty() && created.isEmpty()) {
            return;
        }
        if (!deleted.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Indices to delete policy instances: {}", Arrays.toString(deleted.toArray()));
            }
            for (String index : deleted) {
                if (!scheduledPolicyInstances.containsKey(index)) {
                    LOG.debug("Policy instance for index '{}' does not exist. Skipping delete", index);
                } else {
                    LOG.trace("Deleting policy instance for index '{}'", index);
                    Map.Entry<ScheduledFuture<?>, PolicyInstance> instanceEntry = scheduledPolicyInstances.remove(index);
                    instanceEntry.getKey().cancel(true);
                    if (!instanceEntry.getKey().isCancelled() && !instanceEntry.getKey().isDone()) {
                        LOG.debug("Could not cancel job for deleted index '{}'", index);
                    }
                    instanceEntry.getValue().handleDelete();
                }
            }
        }
        Map<String, PolicyInstanceState> newStates = new HashMap<>();
        Map<String, PolicyInstance> newInstances = new HashMap<>();
        if (!created.isEmpty()) {
            LOG.debug("Indices to schedule policy instances: {}", Arrays.toString(created.keySet().toArray(new String[0])));
            Map<String, PolicyInstanceState> existingStates = policyInstanceService.getStates(created.keySet());
            Set<String> policyNames = new HashSet<>(created.values());
            for (MultiGetItemResponse res : policyService.multiGetPolicy(policyNames)) {
                LOG.trace("Creating instances for policy '{}'", res.getId());
                List<String> correspondingIndices = getKeysWithCorrespondingValue(created, res.getId());
                if (!res.isFailed()) {
                    if (res.getResponse().isExists()) {
                        try {
                            Policy policy = Policy.parse(DocNode.parse(Format.JSON).from(res.getResponse().getSourceAsBytesRef().utf8ToString()),
                                    Policy.ParsingContext.lenient(conditionFactory, actionFactory));
                            for (String index : correspondingIndices) {
                                if (scheduledPolicyInstances.containsKey(index)) {
                                    LOG.trace("PolicyInstance for index '{}' with policy '{}' already exists. Skipping", index, res.getId());
                                    continue;
                                }
                                LOG.trace("Creating instance for index '{}'", index);
                                PolicyInstanceState state;
                                if (existingStates.containsKey(index)
                                        && existingStates.get(index).getStatus() != PolicyInstanceState.Status.DELETED) {
                                    state = existingStates.get(index);
                                } else {
                                    state = new PolicyInstanceState(res.getId());
                                    newStates.put(index, state);
                                }
                                newInstances.put(index, new PolicyInstance(index, policy, state, executionContext));
                            }
                        } catch (ConfigValidationException e) {
                            LOG.warn("Policy '{}' is corrupted", res.getId(), e);
                        }
                    } else {
                        LOG.warn("Could not create policy instance for indices {} with policy '{}' because policy does not exist",
                                Arrays.toString(correspondingIndices.toArray()), res.getId());
                    }
                } else {
                    LOG.warn("Could not create policy instance for indices {} with policy '{}' because of an API error",
                            Arrays.toString(correspondingIndices.toArray()), res.getId(), res.getFailure().getFailure());
                }
            }
        }
        if (!newStates.isEmpty()) {
            policyInstanceService.deleteCreateStates(ImmutableList.empty(), newStates);
        }
        if (!newInstances.isEmpty()) {
            schedulePolicyInstances(newInstances.entrySet());
        }
    }

    private List<String> getKeysWithCorrespondingValue(Map<String, String> map, String value) {
        return map.entrySet().stream().filter(entry -> entry.getValue().equals(value)).map(Map.Entry::getKey).collect(Collectors.toList());
    }

    private synchronized void schedulePolicyInstances(Set<Map.Entry<String, PolicyInstance>> instances) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Scheduling policy instances: {}", Arrays.toString(instances.stream().map(Map.Entry::getKey).toArray()));
        }
        long executionPeriod = settings.getDynamic().getExecutionPeriod().getMillis();
        long executionDelay = settings.getDynamic().getExecutionFixedDelay().getMillis();
        SecureRandom random = new SecureRandom();
        for (Map.Entry<String, PolicyInstance> entry : instances) {
            long delay = executionDelay + (settings.getDynamic().getExecutionRandomDelayEnabled() ? random.nextLong(executionPeriod) : 0);
            ScheduledFuture<?> scheduledFuture = scheduler.scheduleAtFixedRate(entry.getValue(), delay, executionPeriod, TimeUnit.MILLISECONDS);
            scheduledPolicyInstances.put(entry.getKey(), new AbstractMap.SimpleImmutableEntry<>(scheduledFuture, entry.getValue()));
        }
    }

    public boolean policyInstanceExistsForPolicy(String policyName) {
        return scheduledPolicyInstances.values().stream()
                .anyMatch(scheduledFuturePolicyInstanceEntry -> scheduledFuturePolicyInstanceEntry.getValue().getPolicyName().equals(policyName));
    }

    private boolean policyInstanceExistsForIndex(String indexName) {
        return scheduledPolicyInstances.containsKey(indexName);
    }

    public synchronized boolean executeRetryPolicyInstance(String indexName, boolean execute, boolean retry) {
        if (!policyInstanceExistsForIndex(indexName)) {
            return false;
        }
        if (retry) {
            scheduledPolicyInstances.get(indexName).getValue().isRetry(true);
        }
        if (execute) {
            scheduler.execute(scheduledPolicyInstances.get(indexName).getValue());
        }
        return true;
    }
}
