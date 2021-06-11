package com.floragunn.signals.execution;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.script.ScriptService;

import com.floragunn.signals.accounts.AccountRegistry;
import com.floragunn.signals.watch.action.invokers.ActionInvocationType;
import com.floragunn.signals.watch.common.HttpEndpointWhitelist;
import com.floragunn.signals.watch.common.HttpProxyConfig;

public class WatchExecutionContext {

    private final Client client;
    private final NamedXContentRegistry xContentRegistry;
    private final ScriptService scriptService;
    private final Map<String, Object> metadata;
    private final ExecutionEnvironment executionEnvironment;
    private final ActionInvocationType actionInvocationType;
    private final AccountRegistry accountRegistry;
    private final WatchExecutionContextData contextData;
    private final WatchExecutionContextData resolvedContextData;
    private final SimulationMode simulationMode;
    private final HttpEndpointWhitelist httpEndpointWhitelist;
    private final HttpProxyConfig httpProxyConfig;

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public WatchExecutionContext(Client client, ScriptService scriptService, NamedXContentRegistry xContentRegistry, AccountRegistry accountRegistry,
            ExecutionEnvironment scheduled) {
        this(client, scriptService, xContentRegistry, accountRegistry, scheduled, ActionInvocationType.ALERT, null);
    }

    public WatchExecutionContext(Client client, ScriptService scriptService, NamedXContentRegistry xContentRegistry, AccountRegistry accountRegistry,
            ExecutionEnvironment executionEnvironment, ActionInvocationType actionInvocationType, WatchExecutionContextData contextData) {
        this(client, scriptService, xContentRegistry, accountRegistry, executionEnvironment, actionInvocationType, contextData, null,
                SimulationMode.FOR_REAL, null, null);
    }

    public WatchExecutionContext(Client client, ScriptService scriptService, NamedXContentRegistry xContentRegistry, AccountRegistry accountRegistry,
            ExecutionEnvironment executionEnvironment, ActionInvocationType actionInvocationType, WatchExecutionContextData contextData,
            WatchExecutionContextData resolvedContextData, SimulationMode simulationMode, HttpEndpointWhitelist httpEndpointWhitelist, HttpProxyConfig httpProxyConfig) {
        this.client = client;
        this.scriptService = scriptService;
        this.xContentRegistry = xContentRegistry;
        this.metadata = null;
        this.executionEnvironment = executionEnvironment;
        this.actionInvocationType = actionInvocationType;
        this.accountRegistry = accountRegistry;
        this.contextData = contextData;
        this.resolvedContextData = resolvedContextData;
        this.simulationMode = simulationMode;
        this.httpEndpointWhitelist = httpEndpointWhitelist;
        this.httpProxyConfig = httpProxyConfig;
    }

    public Client getClient() {
        return client;
    }

    public NamedXContentRegistry getxContentRegistry() {
        return xContentRegistry;
    }

    public ScriptService getScriptService() {
        return scriptService;
    }

    public ExecutionEnvironment getExecutionEnvironment() {
        return executionEnvironment;
    }

    public AccountRegistry getAccountRegistry() {
        return accountRegistry;
    }

    public WatchExecutionContextData getContextData() {
        return contextData;
    }

    public Map<String, Object> getTemplateScriptParamsAsMap() {
        Map<String, Object> result = new HashMap<>(contextData.getTemplateScriptParamsAsMap());

        if (resolvedContextData != null) {
            result.put("resolved", resolvedContextData.getTemplateScriptParamsAsMap());
        }

        return result;
    }

    public WatchExecutionContext with(WatchExecutionContextData contextData) {
        return new WatchExecutionContext(client, scriptService, xContentRegistry, accountRegistry, executionEnvironment, actionInvocationType,
                contextData, resolvedContextData, simulationMode, httpEndpointWhitelist, httpProxyConfig);
    }

    public WatchExecutionContext with(ActionInvocationType actionInvocationType) {
        return new WatchExecutionContext(client, scriptService, xContentRegistry, accountRegistry, executionEnvironment, actionInvocationType,
                contextData, resolvedContextData, simulationMode, httpEndpointWhitelist, httpProxyConfig);
    }

    public WatchExecutionContext clone() {
        return new WatchExecutionContext(client, scriptService, xContentRegistry, accountRegistry, executionEnvironment, actionInvocationType,
                contextData != null ? contextData.clone() : null, resolvedContextData != null ? resolvedContextData.clone() : null, simulationMode,
                httpEndpointWhitelist, httpProxyConfig);
    }

    public WatchExecutionContextData getResolvedContextData() {
        return resolvedContextData;
    }

    public ActionInvocationType getActionInvocationType() {
        return actionInvocationType;
    }

    public SimulationMode getSimulationMode() {
        return simulationMode;
    }

    public HttpEndpointWhitelist getHttpEndpointWhitelist() {
        return httpEndpointWhitelist;
    }

    public HttpProxyConfig getHttpProxyConfig() {
        return httpProxyConfig;
    }

}
