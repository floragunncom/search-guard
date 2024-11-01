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

package com.floragunn.signals.execution;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import com.floragunn.signals.accounts.AccountRegistry;
import com.floragunn.signals.truststore.service.TrustManagerRegistry;
import com.floragunn.signals.watch.action.invokers.ActionInvocationType;
import com.floragunn.signals.watch.action.invokers.ActionInvoker;
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
    private final String frontendBaseUrl;
    private final ActionInvoker actionInvoker;
    private final TrustManagerRegistry trustManagerRegistry;
    
    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public WatchExecutionContext(Client client, ScriptService scriptService, NamedXContentRegistry xContentRegistry, AccountRegistry accountRegistry,
            ExecutionEnvironment executionEnvironment, ActionInvocationType actionInvocationType, WatchExecutionContextData contextData,
            TrustManagerRegistry trustManagerRegistry) {
        this(client, scriptService, xContentRegistry, accountRegistry, executionEnvironment, actionInvocationType, contextData, null,
                SimulationMode.FOR_REAL, null, null, null, null, trustManagerRegistry);
    }

    public WatchExecutionContext(Client client, ScriptService scriptService, NamedXContentRegistry xContentRegistry, AccountRegistry accountRegistry,
            ExecutionEnvironment executionEnvironment, ActionInvocationType actionInvocationType, WatchExecutionContextData contextData,
            WatchExecutionContextData resolvedContextData, SimulationMode simulationMode, HttpEndpointWhitelist httpEndpointWhitelist,
        HttpProxyConfig httpProxyConfig, String frontendBaseUrl, ActionInvoker actionInvoker, TrustManagerRegistry trustManagerRegistry) {
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
        this.frontendBaseUrl = frontendBaseUrl;
        this.actionInvoker = actionInvoker;
        this.trustManagerRegistry = Objects.requireNonNull(trustManagerRegistry, "Trust manager registry is required");
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

    public TrustManagerRegistry getTrustManagerRegistry() {
        return trustManagerRegistry;
    }

    public WatchExecutionContextData getContextData() {
        return contextData;
    }

    public Map<String, Object> getTemplateScriptParamsAsMap() {
        Map<String, Object> result = new HashMap<>(contextData.getTemplateScriptParamsAsMap());

        if (resolvedContextData != null) {
            result.put("resolved", resolvedContextData.getTemplateScriptParamsAsMap());
        }
        
        if (this.frontendBaseUrl != null && actionInvocationType == ActionInvocationType.ALERT && this.contextData.getWatch() != null) {
            String tenant = this.contextData.getWatch().getTenant();
            
            if ("_main".equals(tenant)) {
                tenant = "SGS_GLOBAL_TENANT";
            }

            result.put("ack_watch_link", this.frontendBaseUrl + "/app/searchguard-signals?sg_tenant=" + tenant + "#/watch/"
                    + this.contextData.getWatch().getId() + "/ack/");

            if (this.actionInvoker != null && this.actionInvoker.getName() != null) {
                result.put("ack_action_link", this.frontendBaseUrl + "/app/searchguard-signals?sg_tenant=" + tenant + "#/watch/"
                        + this.contextData.getWatch().getId() + "/ack/" + this.actionInvoker.getName() + "/");
            }
        }

        return result;
    }

    public WatchExecutionContext with(WatchExecutionContextData contextData, ActionInvoker actionInvoker) {
        return new WatchExecutionContext(client, scriptService, xContentRegistry, accountRegistry, executionEnvironment, actionInvocationType,
                contextData, resolvedContextData, simulationMode, httpEndpointWhitelist, httpProxyConfig, frontendBaseUrl, actionInvoker,
                trustManagerRegistry);
    }

    public WatchExecutionContext with(ActionInvocationType actionInvocationType) {
        return new WatchExecutionContext(client, scriptService, xContentRegistry, accountRegistry, executionEnvironment, actionInvocationType,
                contextData, resolvedContextData, simulationMode, httpEndpointWhitelist, httpProxyConfig, frontendBaseUrl, actionInvoker,
                trustManagerRegistry);
    }

    public WatchExecutionContext clone() {
        return new WatchExecutionContext(client, scriptService, xContentRegistry, accountRegistry, executionEnvironment, actionInvocationType,
                contextData != null ? contextData.clone() : null, resolvedContextData != null ? resolvedContextData.clone() : null, simulationMode,
                httpEndpointWhitelist, httpProxyConfig, frontendBaseUrl, actionInvoker, trustManagerRegistry);
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

    public String getFrontendBaseUrl() {
        return frontendBaseUrl;
    }

}
