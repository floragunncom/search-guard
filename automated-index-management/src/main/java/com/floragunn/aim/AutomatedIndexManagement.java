package com.floragunn.aim;

import com.floragunn.aim.policy.PolicyService;
import com.floragunn.aim.policy.actions.*;
import com.floragunn.aim.policy.conditions.*;
import com.floragunn.aim.policy.instance.PolicyInstanceHandler;
import com.floragunn.aim.policy.instance.PolicyInstanceService;
import com.floragunn.aim.policy.instance.PolicyInstanceStateLogHandler;
import com.floragunn.searchguard.configuration.ProtectedConfigIndexService;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.cstate.ComponentState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class AutomatedIndexManagement extends AbstractLifecycleComponent {
    private static final Logger LOG = LogManager.getLogger(AutomatedIndexManagement.class);

    private final AutomatedIndexManagementSettings aimSettings;
    private final ComponentState componentState;
    private final LocalNodeMasterListener localNodeMasterListener;
    private final AutomatedIndexManagementSettings.Dynamic.ChangeListener settingsChangeListener;
    private final Condition.Factory conditionFactory = Condition.Factory.defaultFactory();
    private final Action.Factory actionFactory = Action.Factory.defaultFactory();

    private Client client;
    private ThreadPool threadPool;
    private ClusterService clusterService;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private PolicyInstanceHandler policyInstanceHandler;
    private PolicyService policyService;
    private PolicyInstanceService policyInstanceService;

    @Inject
    public AutomatedIndexManagement(Settings settings, ComponentState componentState) {
        aimSettings = new AutomatedIndexManagementSettings(settings);
        this.componentState = componentState;
        this.componentState.setState(ComponentState.State.INITIALIZING);
        localNodeMasterListener = new LocalNodeMasterListener() {
            @Override
            public void onMaster() {
                initMaster();
            }

            @Override
            public void offMaster() {
                stopMaster();
            }
        };
        settingsChangeListener = changed -> {
            if (changed.contains(AutomatedIndexManagementSettings.Dynamic.ACTIVE)) {
                if (clusterService.state().nodes().isLocalNodeElectedMaster()) {
                    if (aimSettings.getDynamic().getActive()) {
                        threadPool.generic().submit(this::initMaster);
                    } else {
                        threadPool.generic().submit(this::stopMaster);
                    }
                }
            }
        };
    }

    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
            ProtectedConfigIndexService protectedConfigIndexService, IndexNameExpressionResolver indexNameExpressionResolver) {
        try {
            this.client = client;
            this.threadPool = threadPool;
            this.clusterService = clusterService;
            this.indexNameExpressionResolver = indexNameExpressionResolver;

            initIndices(protectedConfigIndexService);
            return Collections.singleton(this);
        } catch (Exception e) {
            componentState.setState(ComponentState.State.FAILED);
            LOG.error("AIM initialization failed", e);
            throw (RuntimeException) e;
        }
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
        stopMaster();
    }

    public Condition.Factory getConditionFactory() {
        return conditionFactory;
    }

    public Action.Factory getActionFactory() {
        return actionFactory;
    }

    public PolicyInstanceHandler getPolicyInstanceHandler() {
        return policyInstanceHandler;
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

    private void initMaster() {
        if (policyInstanceHandler == null && aimSettings.getDynamic().getStateLogActive()) {
            LOG.info("Starting AIM policy instance handler");
            policyInstanceHandler = new PolicyInstanceHandler(aimSettings, policyService, policyInstanceService, client, threadPool, clusterService,
                    indexNameExpressionResolver, conditionFactory, actionFactory);
            policyInstanceHandler.init();
        }
    }

    private void stopMaster() {
        if (policyInstanceHandler != null) {
            LOG.info("Stopping AIM policy instance handler");
            policyInstanceHandler.stop();
            policyInstanceHandler = null;
        }
    }

    private synchronized void init(ProtectedConfigIndexService.FailureListener failureListener) {
        try {
            if (ComponentState.State.INITIALIZED.equals(componentState.getState())) {
                return;
            }
            LOG.info("Initializing AIM");
            aimSettings.getDynamic().init(PrivilegedConfigClient.adapt(client));
            policyService = new PolicyService(aimSettings, client);
            policyInstanceService = new PolicyInstanceService(aimSettings, client);
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
        AutomatedIndexManagementSettings.Static.ConfigIndices configIndices = aimSettings.getStatic().configIndices();
        componentState.addPart(protectedConfigIndexService.createIndex(new ProtectedConfigIndexService.ConfigIndex(configIndices.getSettingsName())));
        componentState.addPart(protectedConfigIndexService.createIndex(new ProtectedConfigIndexService.ConfigIndex(configIndices.getStatesName())));
        componentState.addPart(protectedConfigIndexService.createIndex(new ProtectedConfigIndexService.ConfigIndex(configIndices.getPoliciesName())
                .dependsOnIndices(configIndices.getSettingsName(), configIndices.getStatesName(), configIndices.getPoliciesName())
                .onIndexReady(this::init)));
    }
}
