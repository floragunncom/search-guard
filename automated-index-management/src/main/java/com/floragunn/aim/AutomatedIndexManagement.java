package com.floragunn.aim;

import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.PolicyService;
import com.floragunn.aim.policy.actions.Action;
import com.floragunn.aim.policy.conditions.Condition;
import com.floragunn.aim.policy.instance.PolicyInstanceManager;
import com.floragunn.aim.policy.instance.PolicyInstanceService;
import com.floragunn.aim.policy.instance.PolicyInstanceState;
import com.floragunn.searchguard.configuration.ProtectedConfigIndexService;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.cstate.ComponentState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.quartz.SchedulerException;

import java.util.Collection;
import java.util.Collections;

public class AutomatedIndexManagement extends AbstractLifecycleComponent {
    public static final Condition.Factory CONDITION_FACTORY = Condition.Factory.defaultFactory();
    public static final Action.Factory ACTION_FACTORY = Action.Factory.defaultFactory();

    private static final Logger LOG = LogManager.getLogger(AutomatedIndexManagement.class);

    private final AutomatedIndexManagementSettings aimSettings;
    private final ComponentState componentState;
    private final LocalNodeMasterListener localNodeMasterListener;
    private final AutomatedIndexManagementSettings.Dynamic.ChangeListener settingsChangeListener;

    private Client client;
    private ClusterService clusterService;
    private PolicyService policyService;
    private PolicyInstanceService policyInstanceService;
    private PolicyInstanceManager policyInstanceManager;

    @Inject
    public AutomatedIndexManagement(Settings settings, ComponentState componentState) {
        aimSettings = new AutomatedIndexManagementSettings(settings);
        this.componentState = componentState;
        this.componentState.setState(ComponentState.State.INITIALIZING);
        localNodeMasterListener = new LocalNodeMasterListener() {
            @Override
            public void onMaster() {
                initMasterAsync();
            }

            @Override
            public void offMaster() {
                stopMaster();
            }
        };
        settingsChangeListener = changed -> {
            if (changed.contains(AutomatedIndexManagementSettings.Dynamic.ACTIVE) && clusterService.state().nodes().isLocalNodeElectedMaster()) {
                if (aimSettings.getDynamic().getActive()) {
                    initMasterAsync();
                } else {
                    stopMaster();
                }
            }
        };
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {
        stopMaster();
    }

    @Override
    protected void doClose() {
        aimSettings.getDynamic().removeChangeListener(settingsChangeListener);
        clusterService.removeListener(localNodeMasterListener);
        stopMaster();
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

    public Condition.Factory getConditionFactory() {
        return CONDITION_FACTORY;
    }

    public Action.Factory getActionFactory() {
        return ACTION_FACTORY;
    }

    public PolicyInstanceManager getPolicyInstanceHandler() {
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

    private synchronized void initMaster() throws SchedulerException {
        if (!policyInstanceManager.isInitialized()) {
            LOG.info("Starting AIM policy instance handler");
            policyInstanceManager.start();
        }
    }

    private void initMasterAsync() {
        new Thread(() -> {
            try {
                initMaster();
            } catch (SchedulerException e) {
                LOG.error("Scheduler failed to start", e);
                componentState.setFailed(e);
            }
        }).start();
    }

    private synchronized void stopMaster() {
        if (policyInstanceManager.isInitialized() && !policyInstanceManager.isShutdown()) {
            LOG.info("Stopping AIM policy instance handler");
            policyInstanceManager.stop();
        }
    }

    private synchronized void init(ProtectedConfigIndexService.FailureListener failureListener) {
        try {
            if (ComponentState.State.INITIALIZED.equals(componentState.getState())) {
                return;
            }
            LOG.info("Initializing AIM");
            aimSettings.getDynamic().init(PrivilegedConfigClient.adapt(client));
            policyService = new PolicyService(client, getConditionFactory(), getActionFactory());
            policyInstanceService = new PolicyInstanceService(client);
            policyInstanceManager = new PolicyInstanceManager(aimSettings, policyService, policyInstanceService, client, clusterService,
                    CONDITION_FACTORY, ACTION_FACTORY);
            if (aimSettings.getDynamic().getActive() && clusterService.state().nodes().isLocalNodeElectedMaster()) {
                initMaster();
            }
            aimSettings.getDynamic().addChangeListener(settingsChangeListener);
            clusterService.addLocalNodeMasterListener(localNodeMasterListener);
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
