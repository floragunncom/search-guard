/*
 * Copyright 2020-2022 floragunn GmbH
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

package com.floragunn.searchguard;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import com.floragunn.codova.validation.VariableResolvers;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.authc.AuthInfoService;
import com.floragunn.searchguard.authc.blocking.BlockedIpRegistry;
import com.floragunn.searchguard.authc.blocking.BlockedUserRegistry;
import com.floragunn.searchguard.authc.internal_users_db.InternalUsersDatabase;
import com.floragunn.searchguard.authz.AuthorizationService;
import com.floragunn.searchguard.authz.PrivilegesEvaluator;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector;
import com.floragunn.searchguard.authz.actions.Actions;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.ProtectedConfigIndexService;
import com.floragunn.searchguard.configuration.StaticSgConfig;
import com.floragunn.searchguard.configuration.variables.ConfigVarService;
import com.floragunn.searchguard.internalauthtoken.InternalAuthTokenProvider;
import com.floragunn.searchguard.license.LicenseRepository;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContextProviderRegistry;
import com.floragunn.searchsupport.StaticSettings;
import com.floragunn.searchsupport.diag.DiagnosticContext;

public class BaseDependencies {

    private final Settings settings;
    private final StaticSettings staticSettings;
    private final Client localClient;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final ResourceWatcherService resourceWatcherService;
    private final ScriptService scriptService;
    private final NamedXContentRegistry xContentRegistry;
    private final Environment environment;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final ConfigurationRepository configurationRepository;
    private final LicenseRepository licenseRepository;
    private final ProtectedConfigIndexService protectedConfigIndexService;
    private final SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry;
    private final NodeEnvironment nodeEnvironment;
    private final InternalAuthTokenProvider internalAuthTokenProvider;
    private final StaticSgConfig staticSgConfig;
    private final DiagnosticContext diagnosticContext;
    private final ConfigVarService configVarService;
    private final AuditLog auditLog;
    private final PrivilegesEvaluator privilegesEvaluator;
    private final BlockedIpRegistry blockedIpRegistry;
    private final BlockedUserRegistry blockedUserRegistry;
    private final SearchGuardModulesRegistry modulesRegistry;
    private final InternalUsersDatabase internalUsersDatabase;
    private final Actions actions;
    private final AuthorizationService authorizationService;
    private final GuiceDependencies guiceDependencies;
    private final AuthInfoService authInfoService;
    private final ActionRequestIntrospector actionRequestIntrospector;

    public BaseDependencies(Settings settings, StaticSettings staticSettings, Client localClient, ClusterService clusterService,
            ThreadPool threadPool, ResourceWatcherService resourceWatcherService, ScriptService scriptService, NamedXContentRegistry xContentRegistry,
            Environment environment, NodeEnvironment nodeEnvironment, IndexNameExpressionResolver indexNameExpressionResolver,
            StaticSgConfig staticSgConfig, ConfigurationRepository configurationRepository, LicenseRepository licenseRepository,
            ProtectedConfigIndexService protectedConfigIndexService, InternalAuthTokenProvider internalAuthTokenProvider,
            SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry, ConfigVarService configVarService, DiagnosticContext diagnosticContext, AuditLog auditLog,
            PrivilegesEvaluator privilegesEvaluator, BlockedIpRegistry blockedIpRegistry, BlockedUserRegistry blockedUserRegistry,
            SearchGuardModulesRegistry modulesRegistry, InternalUsersDatabase internalUsersDatabase, Actions actions,
            AuthorizationService authorizationService, GuiceDependencies guiceDependencies, AuthInfoService authInfoService,
            ActionRequestIntrospector actionRequestIntrospector) {
        super();
        this.settings = settings;
        this.staticSettings = staticSettings;
        this.localClient = localClient;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.resourceWatcherService = resourceWatcherService;
        this.scriptService = scriptService;
        this.xContentRegistry = xContentRegistry;
        this.environment = environment;
        this.nodeEnvironment = nodeEnvironment;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.staticSgConfig = staticSgConfig;
        this.configurationRepository = configurationRepository;
        this.licenseRepository = licenseRepository;
        this.protectedConfigIndexService = protectedConfigIndexService;
        this.specialPrivilegesEvaluationContextProviderRegistry = specialPrivilegesEvaluationContextProviderRegistry;
        this.internalAuthTokenProvider = internalAuthTokenProvider;
        this.configVarService = configVarService;
        this.diagnosticContext = diagnosticContext;
        this.auditLog = auditLog;
        this.privilegesEvaluator = privilegesEvaluator;
        this.blockedIpRegistry = blockedIpRegistry;
        this.blockedUserRegistry = blockedUserRegistry;
        this.modulesRegistry = modulesRegistry;
        this.internalUsersDatabase = internalUsersDatabase;
        this.actions = actions;
        this.authorizationService = authorizationService;
        this.guiceDependencies = guiceDependencies;
        this.authInfoService = authInfoService;
        this.actionRequestIntrospector = actionRequestIntrospector;
    }

    public Settings getSettings() {
        return settings;
    }

    public Client getLocalClient() {
        return localClient;
    }

    public ClusterService getClusterService() {
        return clusterService;
    }

    public ThreadPool getThreadPool() {
        return threadPool;
    }

    public ResourceWatcherService getResourceWatcherService() {
        return resourceWatcherService;
    }

    public ScriptService getScriptService() {
        return scriptService;
    }

    public NamedXContentRegistry getxContentRegistry() {
        return xContentRegistry;
    }

    public Environment getEnvironment() {
        return environment;
    }

    public IndexNameExpressionResolver getIndexNameExpressionResolver() {
        return indexNameExpressionResolver;
    }

    public ConfigurationRepository getConfigurationRepository() {
        return configurationRepository;
    }

    public SpecialPrivilegesEvaluationContextProviderRegistry getSpecialPrivilegesEvaluationContextProviderRegistry() {
        return specialPrivilegesEvaluationContextProviderRegistry;
    }

    public ProtectedConfigIndexService getProtectedConfigIndexService() {
        return protectedConfigIndexService;
    }

    public NodeEnvironment getNodeEnvironment() {
        return nodeEnvironment;
    }

    public InternalAuthTokenProvider getInternalAuthTokenProvider() {
        return internalAuthTokenProvider;
    }

    public StaticSgConfig getStaticSgConfig() {
        return staticSgConfig;
    }

    public DiagnosticContext getDiagnosticContext() {
        return diagnosticContext;
    }

    public ConfigVarService getConfigVarService() {
        return configVarService;
    }

    public AuditLog getAuditLog() {
        return auditLog;
    }

    public PrivilegesEvaluator getPrivilegesEvaluator() {
        return privilegesEvaluator;
    }

    public BlockedIpRegistry getBlockedIpRegistry() {
        return blockedIpRegistry;
    }

    public BlockedUserRegistry getBlockedUserRegistry() {
        return blockedUserRegistry;
    }

    public SearchGuardModulesRegistry getModulesRegistry() {
        return modulesRegistry;
    }

    public InternalUsersDatabase getInternalUsersDatabase() {
        return internalUsersDatabase;
    }

    public Actions getActions() {
        return actions;
    }

    public StaticSettings getStaticSettings() {
        return staticSettings;
    }

    public AuthorizationService getAuthorizationService() {
        return authorizationService;
    }

    public GuiceDependencies getGuiceDependencies() {
        return guiceDependencies;
    }

    public AuthInfoService getAuthInfoService() {
        return authInfoService;
    }

    public LicenseRepository getLicenseRepository() {
        return licenseRepository;
    }

    public ActionRequestIntrospector getActionRequestIntrospector() {
        return actionRequestIntrospector;
    }

}