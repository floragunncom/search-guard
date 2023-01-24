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

import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.authz.AuthorizationService;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.StaticSgConfig;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContextProviderRegistry;
import com.floragunn.searchguard.ssl.transport.DefaultPrincipalExtractor;
import com.floragunn.searchguard.ssl.transport.PrincipalExtractor;
import com.floragunn.searchguard.transport.DefaultInterClusterRequestEvaluator;
import com.floragunn.searchguard.transport.InterClusterRequestEvaluator;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.threadpool.ThreadPool;

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
            StaticSgConfig staticSgConfig, final ClusterService cs, final PrincipalExtractor principalExtractor,
            AuthorizationService authorizationService,
            SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry, final ThreadPool threadPool,
            final AuditLog auditlog) {

        if (enterpriseModulesDisabled()) {
            return Collections.emptyList();
        }

        return instantiateRestApiHandler("com.floragunn.searchguard.dlic.rest.api.SearchGuardRestApiActions", settings, configPath, restController,
                localClient, adminDns, cr, staticSgConfig, cs, principalExtractor, authorizationService,
                specialPrivilegesEvaluationContextProviderRegistry, threadPool, auditlog);
    }

    @SuppressWarnings("unchecked")
    public static Collection<RestHandler> instantiateRestApiHandler(final String className, final Settings settings, final Path configPath,
            final RestController restController, final Client localClient, final AdminDNs adminDns, final ConfigurationRepository cr,
            StaticSgConfig staticSgConfig, final ClusterService cs, final PrincipalExtractor principalExtractor,
            AuthorizationService authorizationService,
            SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry, final ThreadPool threadPool,
            final AuditLog auditlog) {

        try {
            final Class<?> clazz = Class.forName(className);
            Collection<RestHandler> result;
            try {
                result = (Collection<RestHandler>) clazz.getDeclaredMethod("getHandler", Settings.class, Path.class, RestController.class,
                        Client.class, AdminDNs.class, ConfigurationRepository.class, StaticSgConfig.class, ClusterService.class,
                        PrincipalExtractor.class, AuthorizationService.class, SpecialPrivilegesEvaluationContextProviderRegistry.class,
                        ThreadPool.class, AuditLog.class).invoke(null, settings, configPath, restController, localClient, adminDns, cr,
                                staticSgConfig, cs, principalExtractor, authorizationService, specialPrivilegesEvaluationContextProviderRegistry,
                                threadPool, auditlog);
            } catch (NoSuchMethodException e) {
                try {
                    result = (Collection<RestHandler>) clazz.getDeclaredMethod("getHandler", Settings.class, Path.class, RestController.class,
                            Client.class, AdminDNs.class, ConfigurationRepository.class, StaticSgConfig.class, ClusterService.class,
                            PrincipalExtractor.class, AuthorizationService.class, ThreadPool.class, AuditLog.class).invoke(null, settings, configPath,
                                    restController, localClient, adminDns, cr, staticSgConfig, cs, principalExtractor, authorizationService,
                                    threadPool, auditlog);
                } catch (NoSuchMethodException e1) {
                    try {
                        result = (Collection<RestHandler>) clazz.getDeclaredMethod("getHandler", Settings.class, Path.class, RestController.class,
                                Client.class, AdminDNs.class, ConfigurationRepository.class, ClusterService.class, PrincipalExtractor.class,
                                AuthorizationService.class, ThreadPool.class, AuditLog.class).invoke(null, settings, configPath, restController,
                                        localClient, adminDns, cr, cs, principalExtractor, authorizationService, threadPool, auditlog);
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
