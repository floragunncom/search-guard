package com.floragunn.signals;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.floragunn.signals.actions.summary.LoadOperatorSummaryAction;
import com.floragunn.signals.actions.summary.LoadOperatorSummaryHandler;
import com.floragunn.signals.actions.watch.ack.AckWatchAction;
import com.floragunn.signals.actions.watch.ack.TransportAckWatchAction;
import com.floragunn.signals.api.AckAndGetWatchApiAction;
import com.floragunn.signals.proxy.rest.ProxyApi;
import com.floragunn.signals.proxy.rest.TransportProxyUpdatedAction;
import com.floragunn.signals.script.SignalsScriptContextFactory;
import com.floragunn.signals.truststore.rest.DeleteTruststoreAction;
import com.floragunn.signals.truststore.rest.FindAllTruststoresAction;
import com.floragunn.signals.truststore.rest.FindOneTruststoreAction;
import com.floragunn.signals.truststore.rest.CreateOrReplaceTruststoreAction;
import com.floragunn.signals.truststore.rest.TransportTruststoreUpdatedAction;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.BaseDependencies;
import com.floragunn.searchguard.SearchGuardModule;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.configuration.ConfigurationChangeListener;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchsupport.StaticSettings;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.floragunn.searchsupport.jobs.actions.CheckForExecutingTriggerAction;
import com.floragunn.searchsupport.jobs.actions.SchedulerConfigUpdateAction;
import com.floragunn.searchsupport.jobs.actions.TransportCheckForExecutingTriggerAction;
import com.floragunn.searchsupport.jobs.actions.TransportSchedulerConfigUpdateAction;
import com.floragunn.signals.actions.account.config_update.DestinationConfigUpdateAction;
import com.floragunn.signals.actions.account.config_update.TransportDestinationConfigUpdateAction;
import com.floragunn.signals.actions.account.delete.DeleteAccountAction;
import com.floragunn.signals.actions.account.delete.TransportDeleteAccountAction;
import com.floragunn.signals.actions.account.get.GetAccountAction;
import com.floragunn.signals.actions.account.get.TransportGetAccountAction;
import com.floragunn.signals.actions.account.put.PutAccountAction;
import com.floragunn.signals.actions.account.put.TransportPutAccountAction;
import com.floragunn.signals.actions.account.search.SearchAccountAction;
import com.floragunn.signals.actions.account.search.TransportSearchAccountAction;
import com.floragunn.signals.actions.admin.start_stop.StartStopAction;
import com.floragunn.signals.actions.admin.start_stop.TransportStartStopAction;
import com.floragunn.signals.actions.settings.get.GetSettingsAction;
import com.floragunn.signals.actions.settings.get.TransportGetSettingsAction;
import com.floragunn.signals.actions.settings.put.PutSettingsAction;
import com.floragunn.signals.actions.settings.put.TransportPutSettingsAction;
import com.floragunn.signals.actions.settings.update.SettingsUpdateAction;
import com.floragunn.signals.actions.settings.update.TransportSettingsUpdateAction;
import com.floragunn.signals.actions.tenant.start_stop.StartStopTenantAction;
import com.floragunn.signals.actions.tenant.start_stop.TransportStartStopTenantAction;
import com.floragunn.signals.actions.watch.ackandget.AckAndGetWatchAction;
import com.floragunn.signals.actions.watch.ackandget.TransportAckAndGetWatchAction;
import com.floragunn.signals.actions.watch.activate_deactivate.TransportDeActivateWatchAction;
import com.floragunn.signals.actions.watch.delete.DeleteWatchAction;
import com.floragunn.signals.actions.watch.delete.TransportDeleteWatchAction;
import com.floragunn.signals.actions.watch.execute.ExecuteWatchAction;
import com.floragunn.signals.actions.watch.execute.TransportExecuteWatchAction;
import com.floragunn.signals.actions.watch.get.GetWatchAction;
import com.floragunn.signals.actions.watch.get.TransportGetWatchAction;
import com.floragunn.signals.actions.watch.put.PutWatchAction;
import com.floragunn.signals.actions.watch.put.TransportPutWatchAction;
import com.floragunn.signals.actions.watch.search.SearchWatchAction;
import com.floragunn.signals.actions.watch.search.TransportSearchWatchAction;
import com.floragunn.signals.actions.watch.state.get.GetWatchStateAction;
import com.floragunn.signals.actions.watch.state.get.TransportGetWatchStateAction;
import com.floragunn.signals.actions.watch.state.search.SearchWatchStateAction;
import com.floragunn.signals.actions.watch.state.search.TransportSearchWatchStateAction;
import com.floragunn.signals.api.AccountApiAction;
import com.floragunn.signals.api.AckWatchApiAction;
import com.floragunn.signals.api.ConvertWatchApiAction;
import com.floragunn.signals.api.DeActivateGloballyAction;
import com.floragunn.signals.api.DeActivateTenantAction;
import com.floragunn.signals.api.DeActivateWatchAction;
import com.floragunn.signals.api.ExecuteWatchApiAction;
import com.floragunn.signals.api.SearchAccountApiAction;
import com.floragunn.signals.api.SearchWatchApiAction;
import com.floragunn.signals.api.SearchWatchStateApiAction;
import com.floragunn.signals.api.SettingsApiAction;
import com.floragunn.signals.api.WatchApiAction;
import com.floragunn.signals.api.WatchStateApiAction;
import com.floragunn.signals.script.types.SignalsObjectFunctionScript;
import com.floragunn.signals.settings.SignalsSettings;
import com.floragunn.signals.watch.checks.Calc;
import com.floragunn.signals.watch.checks.Condition;
import com.floragunn.signals.watch.checks.Transform;
import com.floragunn.signals.watch.severity.SeverityMapping;

public class SignalsModule implements SearchGuardModule, ComponentStateProvider {

    private final boolean enabled;
    private Signals signals;
    private final ComponentState moduleState = new ComponentState(100, null, "signals", SignalsModule.class);

    public SignalsModule(Settings settings) {
        enabled = settings.getAsBoolean("signals.enabled", true);

        if (!enabled) {
            moduleState.setState(ComponentState.State.DISABLED);
        }
    }

    public SignalsModule() {
        enabled = true;
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController controller, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            ScriptService scriptService, Supplier<DiscoveryNodes> nodesInCluster, Predicate<NodeFeature> clusterSupportsFeature) {
        if (enabled) {
            return Arrays.asList(new WatchApiAction(settings), new ExecuteWatchApiAction(settings, scriptService, this.signals),
                    new DeActivateWatchAction(settings, controller), new AckWatchApiAction(settings, controller), new SearchWatchApiAction(settings, clusterSupportsFeature),
                    new AccountApiAction(settings, controller), new SearchAccountApiAction(clusterSupportsFeature), new WatchStateApiAction(settings, controller),
                    new SettingsApiAction(settings, controller), new DeActivateTenantAction(settings, controller),
                    new DeActivateGloballyAction(settings, controller), new SearchWatchStateApiAction(settings, clusterSupportsFeature), new ConvertWatchApiAction(settings),
                    new AckAndGetWatchApiAction(settings), CreateOrReplaceTruststoreAction.REST_API, FindOneTruststoreAction.REST_API,
                    DeleteTruststoreAction.REST_API, FindAllTruststoresAction.REST_API, ProxyApi.REST_API, LoadOperatorSummaryAction.REST_API
            );
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        if (enabled) {
            return Arrays.asList(new ActionHandler<>(AckAndGetWatchAction.INSTANCE, TransportAckAndGetWatchAction.class),
                new ActionHandler<>(AckWatchAction.INSTANCE, TransportAckWatchAction.class),
                    new ActionHandler<>(GetWatchAction.INSTANCE, TransportGetWatchAction.class),
                    new ActionHandler<>(PutWatchAction.INSTANCE, TransportPutWatchAction.class),
                    new ActionHandler<>(DeleteWatchAction.INSTANCE, TransportDeleteWatchAction.class),
                    new ActionHandler<>(SearchWatchAction.INSTANCE, TransportSearchWatchAction.class),
                    new ActionHandler<>(com.floragunn.signals.actions.watch.activate_deactivate.DeActivateWatchAction.INSTANCE,
                            TransportDeActivateWatchAction.class),
                    new ActionHandler<>(ExecuteWatchAction.INSTANCE, TransportExecuteWatchAction.class),
                    new ActionHandler<>(DestinationConfigUpdateAction.INSTANCE, TransportDestinationConfigUpdateAction.class),
                    new ActionHandler<>(PutAccountAction.INSTANCE, TransportPutAccountAction.class),
                    new ActionHandler<>(GetAccountAction.INSTANCE, TransportGetAccountAction.class),
                    new ActionHandler<>(DeleteAccountAction.INSTANCE, TransportDeleteAccountAction.class),
                    new ActionHandler<>(SearchAccountAction.INSTANCE, TransportSearchAccountAction.class),
                    new ActionHandler<>(GetWatchStateAction.INSTANCE, TransportGetWatchStateAction.class),
                    new ActionHandler<>(SettingsUpdateAction.INSTANCE, TransportSettingsUpdateAction.class),
                    new ActionHandler<>(GetSettingsAction.INSTANCE, TransportGetSettingsAction.class),
                    new ActionHandler<>(PutSettingsAction.INSTANCE, TransportPutSettingsAction.class),
                    new ActionHandler<>(StartStopTenantAction.INSTANCE, TransportStartStopTenantAction.class),
                    new ActionHandler<>(StartStopAction.INSTANCE, TransportStartStopAction.class),
                    new ActionHandler<>(SearchWatchStateAction.INSTANCE, TransportSearchWatchStateAction.class),
                    new ActionHandler<>(SchedulerConfigUpdateAction.INSTANCE, TransportSchedulerConfigUpdateAction.class),
                    new ActionHandler<>(CheckForExecutingTriggerAction.INSTANCE, TransportCheckForExecutingTriggerAction.class),
                    new ActionHandler<>(CreateOrReplaceTruststoreAction.INSTANCE, CreateOrReplaceTruststoreAction.UploadTruststoreHandler.class),
                    new ActionHandler<>(FindOneTruststoreAction.INSTANCE, FindOneTruststoreAction.FindOneTruststoreHandler.class),
                    new ActionHandler<>(FindAllTruststoresAction.INSTANCE, FindAllTruststoresAction.FindAllTruststoresHandler.class),
                    new ActionHandler<>(DeleteTruststoreAction.INSTANCE, DeleteTruststoreAction.DeleteTruststoreHandler.class),
                    new ActionHandler<>(TransportTruststoreUpdatedAction.TruststoreUpdatedActionType.INSTANCE, TransportTruststoreUpdatedAction.class),
                    new ActionHandler<>(ProxyApi.CreateOrReplaceProxyAction.INSTANCE, ProxyApi.CreateOrReplaceProxyAction.CreateOrUpdateProxyHandler.class),
                    new ActionHandler<>(ProxyApi.FindOneProxyAction.INSTANCE, ProxyApi.FindOneProxyAction.FindOneProxyHandler.class),
                    new ActionHandler<>(ProxyApi.DeleteProxyAction.INSTANCE, ProxyApi.DeleteProxyAction.DeleteProxyHandler.class),
                    new ActionHandler<>(ProxyApi.FindAllProxiesAction.INSTANCE, ProxyApi.FindAllProxiesAction.FindAllProxiesHandler.class),
                    new ActionHandler<>(TransportProxyUpdatedAction.ProxyUpdatedActionType.INSTANCE, TransportProxyUpdatedAction.class),
                    new ActionHandler<>(LoadOperatorSummaryAction.INSTANCE, LoadOperatorSummaryHandler.class)
            );
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public List<ScriptContext<?>> getContexts() {
        if (enabled) {
            return Arrays.asList(Condition.ConditionScript.CONTEXT, Transform.TransformScript.CONTEXT, Calc.CalcScript.CONTEXT,
                    SeverityMapping.SeverityValueScript.CONTEXT, SignalsObjectFunctionScript.CONTEXT, SignalsScriptContextFactory.TEMPLATE_CONTEXT);
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public Collection<Object> createComponents(BaseDependencies baseDependencies) {
        if (enabled) {
            Signals signals = new Signals(baseDependencies.getSettings(), moduleState);

            baseDependencies.getConfigurationRepository().subscribeOnChange(new ConfigurationChangeListener() {

                @Override
                public void onChange(ConfigMap configMap) {
                    SgDynamicConfiguration<Tenant> tenants = configMap.get(CType.TENANTS);
                    
                    if (tenants != null) {
                        baseDependencies.getThreadPool().generic().submit(() -> signals.updateTenants(tenants.getCEntries().keySet()));
                    }
                }
            });

            this.signals = signals;

            return signals.createComponents(baseDependencies.getLocalClient(), baseDependencies.getClusterService(), baseDependencies.getThreadPool(),
                    baseDependencies.getScriptService(), baseDependencies.getxContentRegistry(),
                    baseDependencies.getNodeEnvironment(), baseDependencies.getInternalAuthTokenProvider(),
                    baseDependencies.getProtectedConfigIndexService(), baseDependencies.getDiagnosticContext(), baseDependencies.getFeatureService());
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public StaticSettings.AttributeSet getSettings() {
        return SignalsSettings.SignalsStaticSettings.getAvailableSettings();
    }

    @Override
    public void onNodeStarted() {
    }

    @Override
    public ComponentState getComponentState() {
        return moduleState;
    }

    @Override
    public ImmutableSet<String> getCapabilities() {
        return enabled ? ImmutableSet.of("signals") : ImmutableSet.empty();
    }

}
