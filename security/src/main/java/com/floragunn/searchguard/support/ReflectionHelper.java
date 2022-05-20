/*
 * Copyright 2015-2017 floragunn GmbH
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

package com.floragunn.searchguard.support;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.index.IndexService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;

import com.floragunn.searchguard.GuiceDependencies;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.auditlog.NullAuditLog;
import com.floragunn.searchguard.authz.PrivilegesEvaluator;
import com.floragunn.searchguard.compliance.ComplianceConfig;
import com.floragunn.searchguard.compliance.ComplianceIndexingOperationListener;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.DlsFlsRequestValve;
import com.floragunn.searchguard.configuration.StaticSgConfig;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContextProviderRegistry;
import com.floragunn.searchguard.ssl.transport.DefaultPrincipalExtractor;
import com.floragunn.searchguard.ssl.transport.PrincipalExtractor;
import com.floragunn.searchguard.transport.DefaultInterClusterRequestEvaluator;
import com.floragunn.searchguard.transport.InterClusterRequestEvaluator;

public class ReflectionHelper {

    protected static final Logger log = LogManager.getLogger(ReflectionHelper.class);
   
    private static boolean enterpriseModulesDisabled() {
        return !enterpriseModulesEnabled;
    }

    public static void registerMngtRestApiHandler(final Settings settings) {

        if (enterpriseModulesDisabled()) {
            return;
        }

        if (!settings.getAsBoolean("http.enabled", true)) {

            try {
                Class.forName("com.floragunn.searchguard.dlic.rest.api.SearchGuardRestApiActions");
            } catch (final Throwable e) {
                log.warn("Unable to register Rest Management Api Module due to {}", e.toString());
                if (log.isDebugEnabled()) {
                    log.debug("Stacktrace: ", e);
                }
            }
        }
    }

    public static Collection<RestHandler> instantiateMngtRestApiHandler(final Settings settings, final Path configPath,
            final RestController restController, final Client localClient, final AdminDNs adminDns, final ConfigurationRepository cr,
            StaticSgConfig staticSgConfig, final ClusterService cs, final PrincipalExtractor principalExtractor, final PrivilegesEvaluator evaluator,
            SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry, final ThreadPool threadPool,
            final AuditLog auditlog) {

        if (enterpriseModulesDisabled()) {
            return Collections.emptyList();
        }

        return instantiateRestApiHandler("com.floragunn.searchguard.dlic.rest.api.SearchGuardRestApiActions", settings, configPath, restController,
                localClient, adminDns, cr, staticSgConfig, cs, principalExtractor, evaluator, specialPrivilegesEvaluationContextProviderRegistry,
                threadPool, auditlog);
    }

    @SuppressWarnings("unchecked")
    public static Collection<RestHandler> instantiateRestApiHandler(final String className, final Settings settings, final Path configPath,
            final RestController restController, final Client localClient, final AdminDNs adminDns, final ConfigurationRepository cr,
            StaticSgConfig staticSgConfig, final ClusterService cs, final PrincipalExtractor principalExtractor, final PrivilegesEvaluator evaluator,
            SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry, final ThreadPool threadPool,
            final AuditLog auditlog) {

        try {
            final Class<?> clazz = Class.forName(className);
            Collection<RestHandler> result;
            try {
                result = (Collection<RestHandler>) clazz
                        .getDeclaredMethod("getHandler", Settings.class, Path.class, RestController.class, Client.class, AdminDNs.class,
                                ConfigurationRepository.class, StaticSgConfig.class, ClusterService.class, PrincipalExtractor.class,
                                PrivilegesEvaluator.class, SpecialPrivilegesEvaluationContextProviderRegistry.class, ThreadPool.class, AuditLog.class)
                        .invoke(null, settings, configPath, restController, localClient, adminDns, cr, staticSgConfig, cs, principalExtractor,
                                evaluator, specialPrivilegesEvaluationContextProviderRegistry, threadPool, auditlog);
            } catch (NoSuchMethodException e) {
                try {
                    result = (Collection<RestHandler>) clazz.getDeclaredMethod("getHandler", Settings.class, Path.class, RestController.class,
                            Client.class, AdminDNs.class, ConfigurationRepository.class, StaticSgConfig.class, ClusterService.class,
                            PrincipalExtractor.class, PrivilegesEvaluator.class, ThreadPool.class, AuditLog.class).invoke(null, settings, configPath,
                                    restController, localClient, adminDns, cr, staticSgConfig, cs, principalExtractor, evaluator, threadPool,
                                    auditlog);
                } catch (NoSuchMethodException e1) {
                    try {
                        result = (Collection<RestHandler>) clazz.getDeclaredMethod("getHandler", Settings.class, Path.class, RestController.class,
                                Client.class, AdminDNs.class, ConfigurationRepository.class, ClusterService.class, PrincipalExtractor.class,
                                PrivilegesEvaluator.class, ThreadPool.class, AuditLog.class).invoke(null, settings, configPath, restController,
                                        localClient, adminDns, cr, cs, principalExtractor, evaluator, threadPool, auditlog);
                    } catch (NoSuchMethodException e2) {
                        result = (Collection<RestHandler>) clazz.getDeclaredMethod("getHandler", Settings.class, Path.class, RestController.class,
                                Client.class, ClusterService.class, ThreadPool.class)
                                .invoke(null, settings, configPath, restController, localClient, cs, threadPool);
                    }
                }
            }

            if (log.isDebugEnabled()) {
                log.debug("Found " + result.size() + " REST API handlers in " + className);
            }

            return result;
        } catch (final Throwable e) {
            log.warn("Unable to enable REST API module {} due to {}", className,
                    e instanceof InvocationTargetException ? ((InvocationTargetException) e).getTargetException().toString() : e.toString());
            if (log.isDebugEnabled()) {
                log.debug("Stacktrace: ", e);
            }
            return Collections.emptyList();
        }
    }

    @SuppressWarnings("unchecked")
    public static Collection<RestHandler> instantiateRestApiHandler(final String className, final Settings settings, final Path configPath,
            final RestController restController, final Client localClient, final AdminDNs adminDns, final ConfigurationRepository cr,
            final ClusterService cs, ScriptService scriptService, NamedXContentRegistry xContentRegistry, final PrincipalExtractor principalExtractor,
            final PrivilegesEvaluator evaluator, final ThreadPool threadPool, final AuditLog auditlog) {

        try {
            final Class<?> clazz = Class.forName(className);
            Collection<RestHandler> result;
            try {
                result = (Collection<RestHandler>) clazz.getDeclaredMethod("getHandler", Settings.class, Path.class, RestController.class,
                        Client.class, AdminDNs.class, ConfigurationRepository.class, ClusterService.class, PrincipalExtractor.class,
                        PrivilegesEvaluator.class, ThreadPool.class, AuditLog.class).invoke(null, settings, configPath, restController, localClient,
                                adminDns, cr, cs, principalExtractor, evaluator, threadPool, auditlog);
            } catch (NoSuchMethodException e) {
                result = (Collection<RestHandler>) clazz
                        .getDeclaredMethod("getHandler", Settings.class, Path.class, RestController.class, Client.class, ClusterService.class,
                                ScriptService.class, NamedXContentRegistry.class, ThreadPool.class)
                        .invoke(null, settings, configPath, restController, localClient, cs, scriptService, xContentRegistry, threadPool);
            }

            if (log.isDebugEnabled()) {
                log.debug("Found " + result.size() + " REST API handlers in " + className);
            }

            return result;
        } catch (final Throwable e) {
            log.warn("Unable to enable REST API module {} due to {}", className,
                    e instanceof InvocationTargetException ? ((InvocationTargetException) e).getTargetException().toString() : e.toString());
            if (log.isDebugEnabled()) {
                log.debug("Stacktrace: ", e);
            }
            return Collections.emptyList();
        }
    }

    @SuppressWarnings("rawtypes")
    public static Constructor instantiateDlsFlsConstructor() {

        if (enterpriseModulesDisabled()) {
            return null;
        }

        try {
            final Class<?> clazz = Class.forName("com.floragunn.searchguard.dlsfls.lucene.SearchGuardFlsDlsIndexSearcherWrapper");
            final Constructor<?> ret = clazz.getConstructor(IndexService.class, Settings.class, AdminDNs.class, ClusterService.class, AuditLog.class,
                    ComplianceIndexingOperationListener.class, ComplianceConfig.class);
            return ret;
        } catch (final Throwable e) {
            log.warn("Unable to enable DLS/FLS Module due to {}", e instanceof InvocationTargetException ? ((InvocationTargetException) e).getTargetException().toString() : e.toString());
            if (log.isDebugEnabled()) {
                log.debug("Stacktrace: ", e);
            }
            return null;
        }
    }

    public static DlsFlsRequestValve instantiateDlsFlsValve(Settings settings, Client nodeClient, ClusterService clusterService,
            IndexNameExpressionResolver resolver, GuiceDependencies guiceDependencies, NamedXContentRegistry namedXContentRegistry,
            ThreadContext threadContext, ConfigurationRepository configurationRepository, PrivilegesEvaluator privilegesEvaluator) {

        if (enterpriseModulesDisabled()) {
            return new DlsFlsRequestValve.NoopDlsFlsRequestValve();
        }

        try {
            final Class<?> clazz = Class.forName("com.floragunn.searchguard.dlsfls.DlsFlsValveImpl");
            final DlsFlsRequestValve ret = (DlsFlsRequestValve) clazz
                    .getConstructor(Settings.class, Client.class, ClusterService.class, IndexNameExpressionResolver.class, GuiceDependencies.class,
                            NamedXContentRegistry.class, ThreadContext.class, ConfigurationRepository.class, PrivilegesEvaluator.class)
                    .newInstance(settings, nodeClient, clusterService, resolver, guiceDependencies, namedXContentRegistry, threadContext,
                            configurationRepository, privilegesEvaluator);
            return ret;
        } catch (final Throwable e) {
            log.warn("Unable to enable DLS/FLS Valve Module due to {}",
                    e instanceof InvocationTargetException ? ((InvocationTargetException) e).getTargetException().toString() : e.toString());
            if (log.isDebugEnabled()) {
                log.debug("Stacktrace: ", e);
            }
            return new DlsFlsRequestValve.NoopDlsFlsRequestValve();
        }
    }

    public static AuditLog instantiateAuditLog(final Settings settings, final Path configPath, final Client localClient, final ThreadPool threadPool,
            final IndexNameExpressionResolver resolver, final ClusterService clusterService) {

        if (enterpriseModulesDisabled()) {
            return new NullAuditLog();
        }

        try {
            final Class<?> clazz = Class.forName("com.floragunn.searchguard.auditlog.impl.AuditLogImpl");
            final AuditLog impl = (AuditLog) clazz.getConstructor(Settings.class, Path.class, Client.class, ThreadPool.class,
                    IndexNameExpressionResolver.class, ClusterService.class)
                    .newInstance(settings, configPath, localClient, threadPool, resolver, clusterService);
            return impl;
        } catch (final Throwable e) {
            log.warn("Unable to enable Auditlog Module due to {}", e instanceof InvocationTargetException ? ((InvocationTargetException) e).getTargetException().toString() : e.toString());
            if (log.isDebugEnabled()) {
                log.debug("Stacktrace: ", e);
            }

            return new NullAuditLog();
        }
    }

    public static ComplianceIndexingOperationListener instantiateComplianceListener(ComplianceConfig complianceConfig, AuditLog auditlog) {

        if (enterpriseModulesDisabled()) {
            return new ComplianceIndexingOperationListener();
        }

        try {
            final Class<?> clazz = Class.forName("com.floragunn.searchguard.compliance.ComplianceIndexingOperationListenerImpl");
            final ComplianceIndexingOperationListener impl = (ComplianceIndexingOperationListener) clazz
                    .getConstructor(ComplianceConfig.class, AuditLog.class).newInstance(complianceConfig, auditlog);
            //no addLoadedModule(clazz) here because its not a typical module
            //and it is not loaded in every case/on every node
            return impl;
        } catch (final ClassNotFoundException e) {
            //TODO produce a single warn msg, this here is issued for every index
            log.debug("Unable to enable Compliance Module due to {}", e.toString());
            if (log.isDebugEnabled()) {
                log.debug("Stacktrace: ", e);
            }
            return new ComplianceIndexingOperationListener();
        } catch (final Throwable e) {
            log.error("Unable to enable Compliance Module", e);
            return new ComplianceIndexingOperationListener();
        }
    }

    public static InterClusterRequestEvaluator instantiateInterClusterRequestEvaluator(final String clazz, final Settings settings) {

        try {
            final Class<?> clazz0 = Class.forName(clazz);
            final InterClusterRequestEvaluator ret = (InterClusterRequestEvaluator) clazz0.getConstructor(Settings.class).newInstance(settings);
            return ret;
        } catch (final Throwable e) {
            log.warn("Unable to load inter cluster request evaluator '{}' due to {}", clazz, e.toString());
            if (log.isDebugEnabled()) {
                log.debug("Stacktrace: ", e);
            }
            return new DefaultInterClusterRequestEvaluator(settings);
        }
    }

    public static PrincipalExtractor instantiatePrincipalExtractor(final String clazz) {

        try {
            final Class<?> clazz0 = Class.forName(clazz);
            final PrincipalExtractor ret = (PrincipalExtractor) clazz0.getConstructor().newInstance();
            return ret;
        } catch (final Throwable e) {
            log.warn("Unable to load pricipal extractor '{}' due to {}", clazz, e.toString());
            if (log.isDebugEnabled()) {
                log.debug("Stacktrace: ", e);
            }
            return new DefaultPrincipalExtractor();
        }
    }

    private static boolean enterpriseModulesEnabled;

    // TODO static hack
    public static void init(final boolean enterpriseModulesEnabled) {
        ReflectionHelper.enterpriseModulesEnabled = enterpriseModulesEnabled;
    }
}
