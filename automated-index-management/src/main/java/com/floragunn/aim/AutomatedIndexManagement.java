package com.floragunn.aim;

import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.PolicyService;
import com.floragunn.aim.policy.actions.Action;
import com.floragunn.aim.policy.conditions.Condition;
import com.floragunn.aim.policy.instance.PolicyInstanceManager;
import com.floragunn.aim.policy.instance.PolicyInstanceService;
import com.floragunn.aim.policy.instance.PolicyInstanceState;
import com.floragunn.aim.policy.instance.PolicyInstanceStateLogManager;
import com.floragunn.aim.policy.schedule.Schedule;
import com.floragunn.aim.scheduler.DynamicJobDistributor;
import com.floragunn.searchguard.configuration.ProtectedConfigIndexService;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.jobs.cluster.NodeIdComparator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.quartz.SchedulerException;

import java.util.Collection;
import java.util.Collections;

public class AutomatedIndexManagement extends AbstractLifecycleComponent {
    public static final Schedule.Factory SCHEDULE_FACTORY = Schedule.Factory.defaultFactory();
    public static final Condition.Factory CONDITION_FACTORY = Condition.Factory.defaultFactory();
    public static final Action.Factory ACTION_FACTORY = Action.Factory.defaultFactory();

    private static final Logger LOG = LogManager.getLogger(AutomatedIndexManagement.class);

    private final AutomatedIndexManagementSettings aimSettings;
    private final ComponentState componentState;
    private final AutomatedIndexManagementSettings.Dynamic.ChangeListener settingsChangeListener;

    private Client client;
    private ClusterService clusterService;
    private DynamicJobDistributor distributor;
    private PolicyService policyService;
    private PolicyInstanceService policyInstanceService;
    private PolicyInstanceManager policyInstanceManager;
    private PolicyInstanceStateLogManager policyInstanceStateLogManager;

    @Inject
    public AutomatedIndexManagement(Settings settings, ComponentState componentState, NodeEnvironment nodeEnvironment) {
        aimSettings = new AutomatedIndexManagementSettings(settings);
        this.componentState = componentState;
        this.componentState.setState(ComponentState.State.INITIALIZING);
        settingsChangeListener = changed -> {
            if (changed.contains(AutomatedIndexManagementSettings.Dynamic.NODE_FILTER)
                    || changed.contains(AutomatedIndexManagementSettings.Dynamic.ACTIVE)) {
                boolean isReschedule = changed.contains(AutomatedIndexManagementSettings.Dynamic.NODE_FILTER)
                        && distributor.isReschedule(aimSettings.getDynamic().getNodeFilter());
                if (distributor.isThisNodeConfiguredForExecution() && aimSettings.getDynamic().getActive()) {
                    startPolicyInstanceManagementAsync();
                } else {
                    stopPolicyInstanceManagement();
                }
                if (isReschedule && policyInstanceManager.isInitialized() && !policyInstanceManager.isShutdown()) {
                    policyInstanceManager.handleReschedule();
                }
            }
            if (changed.contains(AutomatedIndexManagementSettings.Dynamic.STATE_LOG_ACTIVE) && policyInstanceStateLogManager != null) {
                if (aimSettings.getDynamic().getStateLogActive()) {
                    if (distributor.isThisNodeConfiguredForExecution() && aimSettings.getDynamic().getActive()) {
                        policyInstanceStateLogManager.start();
                    }
                } else {
                    policyInstanceStateLogManager.stop();
                }
            }
        };
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {
        stopPolicyInstanceManagement();
    }

    @Override
    protected void doClose() {
        aimSettings.getDynamic().removeChangeListener(settingsChangeListener);
        stopPolicyInstanceManagement();
    }

    public Collection<Object> createComponents(Client client, ClusterService clusterService,
            ProtectedConfigIndexService protectedConfigIndexService) {
        try {
            this.client = client;
            this.clusterService = clusterService;

            initIndices(protectedConfigIndexService);
            return Collections.singleton(this);
        } catch (Exception e) {
            componentState.setState(ComponentState.State.FAILED);
            LOG.error("AIM initialization failed", e);
            throw (RuntimeException) e;
        }
    }

    public Schedule.Factory getScheduleFactory() {
        return SCHEDULE_FACTORY;
    }

    public Condition.Factory getConditionFactory() {
        return CONDITION_FACTORY;
    }

    public Action.Factory getActionFactory() {
        return ACTION_FACTORY;
    }

    public PolicyInstanceManager getPolicyInstanceManager() {
        return policyInstanceManager;
    }

    public AutomatedIndexManagementSettings getAimSettings() {
        return aimSettings;
    }

    public PolicyService getPolicyService() {
        return policyService;
    }

    public PolicyInstanceService getPolicyInstanceService() {
        return policyInstanceService;
    }

    private synchronized void startPolicyInstanceManagement() throws SchedulerException {
        if (!policyInstanceManager.isInitialized()) {
            LOG.info("Starting AIM policy instance manager");
            policyInstanceManager.start();
        }
        if (policyInstanceStateLogManager != null && aimSettings.getDynamic().getStateLogActive()) {
            policyInstanceStateLogManager.start();
        }
    }

    private void startPolicyInstanceManagementAsync() {
        new Thread(() -> {
            try {
                startPolicyInstanceManagement();
            } catch (SchedulerException e) {
                LOG.error("Scheduler failed to start", e);
                componentState.setFailed(e);
            }
        }).start();
    }

    private synchronized void stopPolicyInstanceManagement() {
        if (policyInstanceManager.isInitialized() && !policyInstanceManager.isShutdown()) {
            LOG.info("Stopping AIM policy instance manager");
            policyInstanceManager.stop();
        }
        if (policyInstanceStateLogManager != null) {
            policyInstanceStateLogManager.stop();
        }
    }

    private synchronized void init(ProtectedConfigIndexService.FailureListener failureListener) {
        try {
            if (ComponentState.State.INITIALIZED.equals(componentState.getState())) {
                return;
            }
            LOG.info("Initializing AIM");
            aimSettings.getDynamic().init(PrivilegedConfigClient.adapt(client));
            distributor = new DynamicJobDistributor("aim_main", new NodeIdComparator(clusterService), aimSettings.getDynamic().getNodeFilter(),
                    clusterService.localNode().getId());
            policyService = new PolicyService(client, getScheduleFactory(), getConditionFactory(), getActionFactory());
            policyInstanceService = new PolicyInstanceService(client);
            policyInstanceManager = new PolicyInstanceManager(aimSettings, policyService, policyInstanceService, client, clusterService, distributor);
            if (aimSettings.getStatic().stateLog().isEnabled()) {
                policyInstanceStateLogManager = new PolicyInstanceStateLogManager(aimSettings, client, clusterService, policyService,
                        policyInstanceService, SCHEDULE_FACTORY, CONDITION_FACTORY, ACTION_FACTORY);
            }
            distributor.initialize();
            if (aimSettings.getDynamic().getActive() && distributor.isThisNodeConfiguredForExecution()) {
                startPolicyInstanceManagement();
            }
            aimSettings.getDynamic().addChangeListener(settingsChangeListener);
            componentState.setState(ComponentState.State.INITIALIZED);
            failureListener.onSuccess();
        } catch (Exception e) {
            componentState.setState(ComponentState.State.FAILED);
            LOG.error("Failed to initialize AIM", e);
            failureListener.onFailure(e);
        }
    }

    private void initIndices(ProtectedConfigIndexService protectedConfigIndexService) {
        componentState.addPart(protectedConfigIndexService
                .createIndex(new ProtectedConfigIndexService.ConfigIndex(AutomatedIndexManagementSettings.ConfigIndices.SETTINGS_NAME)));
        componentState.addPart(protectedConfigIndexService
                .createIndex(new ProtectedConfigIndexService.ConfigIndex(AutomatedIndexManagementSettings.ConfigIndices.POLICY_INSTANCE_STATES_NAME)
                        .mapping(PolicyInstanceState.INDEX_MAPPING)));
        componentState.addPart(protectedConfigIndexService.createIndex(
                new ProtectedConfigIndexService.ConfigIndex(AutomatedIndexManagementSettings.ConfigIndices.POLICY_INSTANCE_TRIGGER_STATES_NAME)));
        componentState.addPart(protectedConfigIndexService
                .createIndex(new ProtectedConfigIndexService.ConfigIndex(AutomatedIndexManagementSettings.ConfigIndices.POLICIES_NAME)
                        .mapping(Policy.INDEX_MAPPING)
                        .dependsOnIndices(AutomatedIndexManagementSettings.ConfigIndices.SETTINGS_NAME,
                                AutomatedIndexManagementSettings.ConfigIndices.POLICY_INSTANCE_STATES_NAME,
                                AutomatedIndexManagementSettings.ConfigIndices.POLICY_INSTANCE_TRIGGER_STATES_NAME,
                                AutomatedIndexManagementSettings.ConfigIndices.POLICIES_NAME)
                        .onIndexReady(this::init)));
    }
}
