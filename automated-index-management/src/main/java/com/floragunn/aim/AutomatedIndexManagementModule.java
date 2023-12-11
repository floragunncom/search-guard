package com.floragunn.aim;

import com.floragunn.aim.api.internal.InternalPolicyAPI;
import com.floragunn.aim.api.internal.InternalPolicyInstanceAPI;
import com.floragunn.aim.api.internal.InternalSettingsAPI;
import com.floragunn.aim.api.rest.PolicyAPI;
import com.floragunn.aim.api.rest.PolicyInstanceAPI;
import com.floragunn.aim.api.rest.SettingsAPI;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.BaseDependencies;
import com.floragunn.searchguard.SearchGuardModule;
import com.floragunn.searchsupport.StaticSettings;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;

import java.util.*;
import java.util.function.Supplier;

public class AutomatedIndexManagementModule implements SearchGuardModule, ComponentStateProvider {
    private final boolean enabled;
    private final ComponentState componentState = new ComponentState(100, null, "aim", AutomatedIndexManagementModule.class);

    public AutomatedIndexManagementModule(Settings settings) {
        enabled = settings.getAsBoolean(AutomatedIndexManagementSettings.Static.ENABLED.name(),
                AutomatedIndexManagementSettings.Static.DEFAULT_ENABLED);
        if (!enabled) {
            componentState.setState(ComponentState.State.DISABLED);
        }
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            ScriptService scriptService, Supplier<DiscoveryNodes> nodesInCluster) {
        if (enabled) {
            return Arrays.asList(PolicyAPI.REST, PolicyInstanceAPI.REST, SettingsAPI.REST);
        }
        return Collections.emptyList();
    }

    @Override
    public List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        if (enabled) {
            List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> handlers = new ArrayList<>();
            handlers.addAll(InternalPolicyAPI.HANDLERS);
            handlers.addAll(InternalPolicyInstanceAPI.HANDLERS);
            handlers.addAll(InternalSettingsAPI.HANDLERS);
            handlers.addAll(PolicyAPI.HANDLERS);
            handlers.addAll(PolicyInstanceAPI.HANDLERS);
            handlers.addAll(SettingsAPI.HANDLERS);
            return ImmutableList.of(handlers);
        }
        return Collections.emptyList();
    }

    @Override
    public Collection<Object> createComponents(BaseDependencies baseDependencies) {
        if (enabled) {
            return new AutomatedIndexManagement(baseDependencies.getSettings(), componentState).createComponents(baseDependencies.getLocalClient(),
                    baseDependencies.getClusterService(), baseDependencies.getThreadPool(), baseDependencies.getProtectedConfigIndexService(),
                    baseDependencies.getIndexNameExpressionResolver());
        }
        return Collections.emptyList();
    }

    @Override
    public StaticSettings.AttributeSet getSettings() {
        return AutomatedIndexManagementSettings.Static.getAvailableSettings();
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }
}
