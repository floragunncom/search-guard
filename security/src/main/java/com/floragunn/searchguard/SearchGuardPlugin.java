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

package com.floragunn.searchguard;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.security.AccessController;
import java.security.MessageDigest;
import java.security.PrivilegedAction;
import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.MultiTenancyChecker.IndexRepository;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.configuration.ClusterInfoHolder;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.ProtectedConfigIndexService;
import com.floragunn.searchguard.configuration.StaticSgConfig;
import com.floragunn.searchguard.configuration.validation.ConfigModificationValidators;
import com.floragunn.searchguard.configuration.validation.RoleRelationsValidator;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Weight;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.TransportSearchScrollAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.env.Environment;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.http.HttpPreRequest;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.HttpServerTransport.Dispatcher;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.cache.query.QueryCache;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.FieldPredicate;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.ScrollContext;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.Transport.Connection;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import com.floragunn.codova.config.text.Pattern;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.SearchGuardModule.QueryCacheWeightProvider;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateAction;
import com.floragunn.searchguard.action.configupdate.TransportConfigUpdateAction;
import com.floragunn.searchguard.action.whoami.TransportWhoAmIAction;
import com.floragunn.searchguard.action.whoami.WhoAmIAction;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.auditlog.AuditLog.Origin;
import com.floragunn.searchguard.auditlog.AuditLogRelay;
import com.floragunn.searchguard.auditlog.AuditLogSslExceptionHandler;
import com.floragunn.searchguard.authc.AuthInfoService;
import com.floragunn.searchguard.authc.blocking.BlockedIpRegistry;
import com.floragunn.searchguard.authc.blocking.BlockedUserRegistry;
import com.floragunn.searchguard.authc.internal_users_db.InternalUsersAuthenticationBackend;
import com.floragunn.searchguard.authc.internal_users_db.InternalUsersConfigApi;
import com.floragunn.searchguard.authc.internal_users_db.InternalUsersDatabase;
import com.floragunn.searchguard.authc.rest.AuthcCacheApi;
import com.floragunn.searchguard.authc.rest.AuthenticatingRestFilter;
import com.floragunn.searchguard.authc.rest.RestAuthcConfigApi;
import com.floragunn.searchguard.authc.session.FrontendAuthcConfigApi;
import com.floragunn.searchguard.authc.session.GetActivatedFrontendConfigAction;
import com.floragunn.searchguard.authc.session.backend.SessionModule;
import com.floragunn.searchguard.authz.AuthorizationService;
import com.floragunn.searchguard.authz.PrivilegesEvaluator;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector;
import com.floragunn.searchguard.authz.actions.Actions;
import com.floragunn.searchguard.authz.config.AuthorizationConfigApi;
import com.floragunn.searchguard.authz.indices.SearchGuardDirectoryReaderWrapper;
import com.floragunn.searchguard.compliance.ComplianceConfig;
import com.floragunn.searchguard.configuration.api.BulkConfigApi;
import com.floragunn.searchguard.configuration.api.GenericTypeLevelConfigApi;
import com.floragunn.searchguard.configuration.api.MigrateConfigIndexApi;
import com.floragunn.searchguard.configuration.variables.ConfigVarApi;
import com.floragunn.searchguard.configuration.variables.ConfigVarRefreshAction;
import com.floragunn.searchguard.configuration.variables.ConfigVarService;
import com.floragunn.searchguard.configuration.variables.EncryptionKeys;
import com.floragunn.searchguard.filter.SearchGuardFilter;
import com.floragunn.searchguard.http.SearchGuardHttpServerTransport;
import com.floragunn.searchguard.http.SearchGuardNonSslHttpServerTransport;
import com.floragunn.searchguard.internalauthtoken.InternalAuthTokenProvider;
import com.floragunn.searchguard.license.LicenseRepository;
import com.floragunn.searchguard.license.SearchGuardLicenseInfoAction;
import com.floragunn.searchguard.license.SearchGuardLicenseKeyApi;
import com.floragunn.searchguard.modules.api.ComponentStateRestAction;
import com.floragunn.searchguard.modules.api.GetComponentStateAction;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContextProviderRegistry;
import com.floragunn.searchguard.privileges.extended_action_handling.ExtendedActionHandlingService;
import com.floragunn.searchguard.privileges.extended_action_handling.ResourceOwnerService;
import com.floragunn.searchguard.rest.KibanaInfoAction;
import com.floragunn.searchguard.rest.PermissionAction;
import com.floragunn.searchguard.rest.SSLReloadCertAction;
import com.floragunn.searchguard.rest.SearchGuardConfigUpdateAction;
import com.floragunn.searchguard.rest.SearchGuardHealthAction;
import com.floragunn.searchguard.rest.SearchGuardInfoAction;
import com.floragunn.searchguard.rest.SearchGuardWhoAmIAction;
import com.floragunn.searchguard.ssl.SearchGuardSSLPlugin;
import com.floragunn.searchguard.ssl.SslExceptionHandler;
import com.floragunn.searchguard.ssl.http.netty.ValidatingDispatcher;
import com.floragunn.searchguard.ssl.transport.SearchGuardSSLNettyTransport;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.HeaderHelper;
import com.floragunn.searchguard.support.ReflectionHelper;
import com.floragunn.searchguard.transport.DefaultInterClusterRequestEvaluator;
import com.floragunn.searchguard.transport.InterClusterRequestEvaluator;
import com.floragunn.searchguard.transport.SearchGuardInterceptor;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.diag.DiagnosticContext;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public final class SearchGuardPlugin extends SearchGuardSSLPlugin implements ClusterPlugin, MapperPlugin, ScriptPlugin {

    private volatile AuthenticatingRestFilter searchGuardRestFilter;
    private volatile SearchGuardInterceptor sgi;
    private AuthorizationService authorizationService;
    private volatile PrivilegesEvaluator evaluator;
    private volatile ThreadPool threadPool;
    private volatile ConfigurationRepository cr;
    private volatile AdminDNs adminDns;
    private volatile ClusterService clusterService;
    private final AuditLogRelay auditLog = new AuditLogRelay();
    private volatile SslExceptionHandler sslExceptionHandler;
    private volatile Client localClient;
    private final boolean disabled;
    private final boolean enterpriseModulesEnabled;
    private final boolean sslOnly;
    private boolean sslCertReloadEnabled;
    private final List<String> demoCertHashes = new ArrayList<String>(3);
    private volatile ComplianceConfig complianceConfig;
    private volatile ActionRequestIntrospector actionRequestIntrospector;
    private ScriptService scriptService;

    private static ProtectedIndices protectedIndices;
    private ProtectedConfigIndexService protectedConfigIndexService;
    private SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry = new SpecialPrivilegesEvaluationContextProviderRegistry();

    private SearchGuardModulesRegistry moduleRegistry;
    private SearchGuardCapabilities capabilities;
    private StaticSgConfig staticSgConfig;
    private AuthInfoService authInfoService;
    private DiagnosticContext diagnosticContext;
    private ConfigVarService configVarService;
    private LicenseRepository licenseRepository;
    private Actions actions;
    private NamedXContentRegistry xContentRegistry;
    private ConfigModificationValidators configModificationValidators;

    @Override
    public void close() throws IOException {
        if (auditLog != null) {
            try {
                auditLog.close();
            } catch (Exception e) {
                log.error("Error while closing auditLog", e);
            }
        }
    }

    private final SslExceptionHandler evaluateSslExceptionHandler() {
        if (disabled || sslOnly) {
            return new SslExceptionHandler() {
            };
        }

        return Objects.requireNonNull(sslExceptionHandler);
    }

    private static boolean isDisabled(final Settings settings) {
        return settings.getAsBoolean(ConfigConstants.SEARCHGUARD_DISABLED, false);
    }

    private static boolean isSslOnlyMode(final Settings settings) {
        return settings.getAsBoolean(ConfigConstants.SEARCHGUARD_SSL_ONLY, false);
    }

    private static boolean isSslCertReloadEnabled(final Settings settings) {
        return settings.getAsBoolean(ConfigConstants.SEARCHGUARD_SSL_CERT_RELOAD_ENABLED, false);
    }

    public SearchGuardPlugin(final Settings settings, final Path configPath) {
        super(settings, configPath, isDisabled(settings));

        disabled = isDisabled(settings);
        sslCertReloadEnabled = isSslCertReloadEnabled(settings);
        moduleRegistry = new SearchGuardModulesRegistry(settings);

        if (disabled) {
            this.enterpriseModulesEnabled = false;
            this.sslOnly = false;
            this.sslCertReloadEnabled = false;
            complianceConfig = null;
            SearchGuardPlugin.protectedIndices = new ProtectedIndices();
            log.warn("Search Guard plugin installed but disabled. This can expose your configuration (including passwords) to the public.");
            return;
        }

        sslOnly = isSslOnlyMode(settings);

        if (sslOnly) {
            this.enterpriseModulesEnabled = false;
            this.sslCertReloadEnabled = false;
            complianceConfig = null;
            SearchGuardPlugin.protectedIndices = new ProtectedIndices();
            log.warn("Search Guard plugin run in ssl only mode. No authentication or authorization is performed");
            return;
        }

        SearchGuardPlugin.protectedIndices = new ProtectedIndices(settings);
        staticSgConfig = new StaticSgConfig(settings);

        demoCertHashes.add("54a92508de7a39d06242a0ffbf59414d7eb478633c719e6af03938daf6de8a1a");
        demoCertHashes.add("742e4659c79d7cad89ea86aab70aea490f23bbfc7e72abd5f0a5d3fb4c84d212");
        demoCertHashes.add("db1264612891406639ecd25c894f256b7c5a6b7e1d9054cbe37b77acd2ddd913");
        demoCertHashes.add("2a5398e20fcb851ec30aa141f37233ee91a802683415be2945c3c312c65c97cf");
        demoCertHashes.add("33129547ce617f784c04e965104b2c671cce9e794d1c64c7efe58c77026246ae");
        demoCertHashes.add("c4af0297cc75546e1905bdfe3934a950161eee11173d979ce929f086fdf9794d");
        demoCertHashes.add("7a355f42c90e7543a267fbe3976c02f619036f5a34ce712995a22b342d83c3ce");
        demoCertHashes.add("a9b5eca1399ec8518081c0d4a21a34eec4589087ce64c04fb01a488f9ad8edc9");

        //new certs 04/2018
        demoCertHashes.add("d14aefe70a592d7a29e14f3ff89c3d0070c99e87d21776aa07d333ee877e758f");
        demoCertHashes.add("54a70016e0837a2b0c5658d1032d7ca32e432c62c55f01a2bf5adcb69a0a7ba9");
        demoCertHashes.add("bdc141ab2272c779d0f242b79063152c49e1b06a2af05e0fd90d505f2b44d5f5");
        demoCertHashes.add("3e839e2b059036a99ee4f742814995f2fb0ced7e9d68a47851f43a3c630b5324");
        demoCertHashes.add("9b13661c073d864c28ad7b13eda67dcb6cbc2f04d116adc7c817c20b4c7ed361");

        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        AccessController.doPrivileged(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                if (Security.getProvider("BC") == null) {
                    Security.addProvider(new BouncyCastleProvider());
                }
                return null;
            }
        });

        enterpriseModulesEnabled = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_ENTERPRISE_MODULES_ENABLED, true);
        ReflectionHelper.init(enterpriseModulesEnabled);

        ReflectionHelper.registerMngtRestApiHandler(settings);

        log.info("Clustername: {}", settings.get("cluster.name", "elasticsearch"));

        if (!transportSSLEnabled && !sslOnly) {
            throw new IllegalStateException(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENABLED + " must be set to 'true'");
        }


        final List<Path> filesWithWrongPermissions = AccessController.doPrivileged(new PrivilegedAction<List<Path>>() {
            @Override
            public List<Path> run() {
                final Path confPath = new Environment(settings, configPath).configFile().toAbsolutePath();
                if (Files.isDirectory(confPath, LinkOption.NOFOLLOW_LINKS)) {
                    try (Stream<Path> s = Files.walk(confPath)) {
                        return s.distinct().filter(p -> checkFilePermissions(p)).collect(Collectors.toList());
                    } catch (Exception e) {
                        log.error(e);
                        return null;
                    }
                }
                return Collections.emptyList();
            }
        });

        if (filesWithWrongPermissions != null && filesWithWrongPermissions.size() > 0) {
            for (final Path p : filesWithWrongPermissions) {
                if (Files.isDirectory(p, LinkOption.NOFOLLOW_LINKS)) {
                    log.warn("Directory " + p + " has insecure file permissions (should be 0700)");
                } else {
                    log.warn("File " + p + " has insecure file permissions (should be 0600)");
                }
            }
        }

        if (!settings.getAsBoolean(ConfigConstants.SEARCHGUARD_ALLOW_UNSAFE_DEMOCERTIFICATES, false)) {
            //check for demo certificates
            final List<String> files = AccessController.doPrivileged(new PrivilegedAction<List<String>>() {
                @Override
                public List<String> run() {
                    final Path confPath = new Environment(settings, configPath).configFile().toAbsolutePath();
                    if (Files.isDirectory(confPath, LinkOption.NOFOLLOW_LINKS)) {
                        try (Stream<Path> s = Files.walk(confPath)) {
                            return s.distinct().map(p -> sha256(p)).collect(Collectors.toList());
                        } catch (Exception e) {
                            log.error(e);
                            return null;
                        }
                    }

                    return Collections.emptyList();
                }
            });

            if (files != null) {
                demoCertHashes.retainAll(files);
                if (!demoCertHashes.isEmpty()) {
                    log.error("Demo certificates found but " + ConfigConstants.SEARCHGUARD_ALLOW_UNSAFE_DEMOCERTIFICATES + " is set to false."
                            + "See http://docs.search-guard.com/latest/demo-installer-generated-artefacts#allow-demo-certificates-and-auto-initialization");
                    throw new RuntimeException("Demo certificates found " + demoCertHashes);
                }
            } else {
                throw new RuntimeException("Unable to look for demo certificates");
            }

        }

        if (enterpriseModulesEnabled) {
            ImmutableSet<String> enterpriseModules = ImmutableSet.of("com.floragunn.searchguard.enterprise.auth.EnterpriseAuthFeaturesModule",
                    "com.floragunn.searchguard.authtoken.AuthTokenModule", "com.floragunn.dlic.auth.LegacyEnterpriseSecurityModule",
                    "com.floragunn.searchguard.enterprise.femt.FeMultiTenancyModule", "com.floragunn.searchguard.enterprise.dlsfls.DlsFlsModule",
                    "com.floragunn.searchguard.enterprise.dlsfls.legacy.LegacyDlsFlsModule",
                    "com.floragunn.searchguard.enterprise.auditlog.AuditLogModule");
            if (! settings.getAsBoolean("searchguard.unsupported.single_index_mt_enabled", true)) {
                enterpriseModules = enterpriseModules.without("com.floragunn.searchguard.enterprise.femt.FeMultiTenancyModule");
            }
            moduleRegistry.add(enterpriseModules.toArray(new String[] {}));
        }

        moduleRegistry.add("com.floragunn.aim.AutomatedIndexManagementModule");
        moduleRegistry.add(SessionModule.class.getName());
        moduleRegistry.add("com.floragunn.signals.SignalsModule");
        moduleRegistry.add("com.floragunn.searchguard.legacy.LegacySecurityModule");
    }

    private String sha256(Path p) {

        if (!Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS)) {
            return "";
        }

        if (!Files.isReadable(p)) {
            log.debug("Unreadable file " + p + " found");
            return "";
        }

        if (!FileSystems.getDefault().getPathMatcher("regex:(?i).*\\.(pem|jks|pfx|p12)").matches(p)) {
            log.debug("Not a .pem, .jks, .pfx or .p12 file, skipping");
            return "";
        }

        try {
            MessageDigest digester = MessageDigest.getInstance("SHA256");
            final String hash = org.bouncycastle.util.encoders.Hex.toHexString(digester.digest(Files.readAllBytes(p)));
            log.debug(hash + " :: " + p);
            return hash;
        } catch (Exception e) {
            throw new ElasticsearchSecurityException("Unable to digest file " + p, e);
        }
    }

    private boolean checkFilePermissions(final Path p) {

        if (p == null) {
            return false;
        }

        Set<PosixFilePermission> perms;

        try {
            perms = Files.getPosixFilePermissions(p, LinkOption.NOFOLLOW_LINKS);
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("Cannot determine posix file permissions for {} due to {}", p, e);
            }
            //ignore, can happen on windows
            return false;
        }

        if (Files.isDirectory(p, LinkOption.NOFOLLOW_LINKS)) {
            if (perms.contains(PosixFilePermission.OTHERS_EXECUTE)) {
                // no x for others must be set
                return true;
            }
        } else {
            if (perms.contains(PosixFilePermission.OWNER_EXECUTE) || perms.contains(PosixFilePermission.GROUP_EXECUTE)
                    || perms.contains(PosixFilePermission.OTHERS_EXECUTE)) {
                // no x must be set
                return true;
            }
        }

        if (perms.contains(PosixFilePermission.OTHERS_READ) || perms.contains(PosixFilePermission.OTHERS_WRITE)) {
            // no permissions for "others" allowed
            return true;
        }

        return false;
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, NamedWriteableRegistry namedWriteableRegistry, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster, Predicate<NodeFeature> clusterSupportsFeature) {

        final List<RestHandler> handlers = new ArrayList<RestHandler>();

        if (!disabled) {

            handlers.addAll(super.getRestHandlers(settings, namedWriteableRegistry, restController, clusterSettings, indexScopedSettings, settingsFilter,
                    indexNameExpressionResolver, nodesInCluster, clusterSupportsFeature));

            if (!sslOnly) {
                handlers.add(
                        new SearchGuardInfoAction(settings, restController, authorizationService, Objects.requireNonNull(moduleRegistry.getTenantAccessMapper()), Objects.requireNonNull(threadPool), clusterService, adminDns));
                handlers.add(new KibanaInfoAction(
                        Objects.requireNonNull(evaluator), Objects.requireNonNull(threadPool),
                        Objects.requireNonNull(moduleRegistry.getMultiTenancyConfigurationProvider())
                ));
                handlers.add(new SearchGuardHealthAction(settings, restController, cr));
                handlers.add(new PermissionAction(settings, restController, Objects.requireNonNull(evaluator), Objects.requireNonNull(threadPool)));

                handlers.addAll(ReflectionHelper.instantiateMngtRestApiHandler(settings, configPath, restController, localClient, adminDns, cr,
                        staticSgConfig, clusterService, Objects.requireNonNull(principalExtractor), authorizationService, specialPrivilegesEvaluationContextProviderRegistry,
                        threadPool, Objects.requireNonNull(auditLog), Objects.requireNonNull(configModificationValidators)));

                handlers.add(new SSLReloadCertAction(sgks, Objects.requireNonNull(threadPool), adminDns, sslCertReloadEnabled));
                handlers.add(new ComponentStateRestAction());
                handlers.add(BulkConfigApi.REST_API);
                handlers.add(GenericTypeLevelConfigApi.REST_API);
                handlers.add(ConfigVarApi.REST_API);
                handlers.add(InternalUsersConfigApi.REST_API);
                handlers.add(RestAuthcConfigApi.REST_API);
                handlers.add(AuthorizationConfigApi.REST_API);
                handlers.add(new AuthcCacheApi.RestHandler());
                handlers.add(FrontendAuthcConfigApi.TypeLevel.REST_API);
                handlers.add(FrontendAuthcConfigApi.DocumentLevel.REST_API);
                handlers.add(SearchGuardLicenseKeyApi.REST_API);
                handlers.add(SearchGuardLicenseInfoAction.REST_API);
                handlers.add(SearchGuardCapabilities.GetCapabilitiesAction.REST_API);       
                handlers.add(ProtectedConfigIndexService.TriggerConfigIndexCreationAction.REST_API);
                handlers.add(GetActivatedFrontendConfigAction.REST_API);
                handlers.add(MigrateConfigIndexApi.REST_API);
                handlers.add(new AuthenticatingRestFilter.DebugApi());

                handlers.add(new SearchGuardWhoAmIAction(settings, adminDns, configPath, principalExtractor));
                handlers.add(new SearchGuardConfigUpdateAction(settings, threadPool, adminDns, configPath, principalExtractor));
            }

            handlers.addAll(moduleRegistry.getRestHandlers(settings, restController, clusterSettings, indexScopedSettings, settingsFilter,
                    indexNameExpressionResolver, scriptService, nodesInCluster, clusterSupportsFeature));
        }

        return handlers;
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = new ArrayList<>(1);
        if (!disabled && !sslOnly) {
            actions.add(new ActionHandler<>(ConfigUpdateAction.INSTANCE, TransportConfigUpdateAction.class));
            actions.add(new ActionHandler<>(WhoAmIAction.INSTANCE, TransportWhoAmIAction.class));
            actions.add(new ActionHandler<>(GetComponentStateAction.INSTANCE, GetComponentStateAction.TransportAction.class));
            actions.add(new ActionHandler<>(SearchGuardLicenseInfoAction.INSTANCE, SearchGuardLicenseInfoAction.Handler.class));
            actions.add(new ActionHandler<>(BulkConfigApi.GetAction.INSTANCE, BulkConfigApi.GetAction.Handler.class));
            actions.add(new ActionHandler<>(BulkConfigApi.UpdateAction.INSTANCE, BulkConfigApi.UpdateAction.Handler.class));
            actions.add(new ActionHandler<>(GenericTypeLevelConfigApi.DeleteAction.INSTANCE, GenericTypeLevelConfigApi.DeleteAction.Handler.class));
            actions.add(new ActionHandler<>(ConfigVarRefreshAction.INSTANCE, ConfigVarRefreshAction.TransportAction.class));
            actions.add(new ActionHandler<>(ConfigVarApi.GetAction.INSTANCE, ConfigVarApi.GetAction.Handler.class));
            actions.add(new ActionHandler<>(ConfigVarApi.UpdateAction.INSTANCE, ConfigVarApi.UpdateAction.Handler.class));
            actions.add(new ActionHandler<>(ConfigVarApi.DeleteAction.INSTANCE, ConfigVarApi.DeleteAction.Handler.class));
            actions.add(new ActionHandler<>(ConfigVarApi.GetAllAction.INSTANCE, ConfigVarApi.GetAllAction.Handler.class));
            actions.add(new ActionHandler<>(ConfigVarApi.UpdateAllAction.INSTANCE, ConfigVarApi.UpdateAllAction.Handler.class));
            actions.add(new ActionHandler<>(InternalUsersConfigApi.GetAction.INSTANCE, InternalUsersConfigApi.GetAction.Handler.class));
            actions.add(new ActionHandler<>(InternalUsersConfigApi.DeleteAction.INSTANCE, InternalUsersConfigApi.DeleteAction.Handler.class));
            actions.add(new ActionHandler<>(InternalUsersConfigApi.PutAction.INSTANCE, InternalUsersConfigApi.PutAction.Handler.class));
            actions.add(new ActionHandler<>(InternalUsersConfigApi.PatchAction.INSTANCE, InternalUsersConfigApi.PatchAction.Handler.class));
            actions.add(new ActionHandler<>(GetActivatedFrontendConfigAction.INSTANCE, GetActivatedFrontendConfigAction.Handler.class));
            actions.add(new ActionHandler<>(RestAuthcConfigApi.GetAction.INSTANCE, RestAuthcConfigApi.GetAction.Handler.class));
            actions.add(new ActionHandler<>(RestAuthcConfigApi.PutAction.INSTANCE, RestAuthcConfigApi.PutAction.Handler.class));
            actions.add(new ActionHandler<>(RestAuthcConfigApi.PatchAction.INSTANCE, RestAuthcConfigApi.PatchAction.Handler.class));
            actions.add(new ActionHandler<>(RestAuthcConfigApi.DeleteAction.INSTANCE, RestAuthcConfigApi.DeleteAction.Handler.class));
            actions.add(new ActionHandler<>(AuthcCacheApi.DeleteAction.INSTANCE, AuthcCacheApi.DeleteAction.TransportAction.class));
            actions.addAll(AuthorizationConfigApi.ACTION_HANDLERS);
            actions.addAll(SearchGuardLicenseKeyApi.ACTION_HANDLERS);
            actions.add(new ActionHandler<>(FrontendAuthcConfigApi.TypeLevel.GetAction.INSTANCE, FrontendAuthcConfigApi.TypeLevel.GetAction.Handler.class));
            actions.add(new ActionHandler<>(FrontendAuthcConfigApi.TypeLevel.PutAction.INSTANCE, FrontendAuthcConfigApi.TypeLevel.PutAction.Handler.class));
            actions.add(new ActionHandler<>(FrontendAuthcConfigApi.TypeLevel.PatchAction.INSTANCE, FrontendAuthcConfigApi.TypeLevel.PatchAction.Handler.class));
            actions.add(new ActionHandler<>(FrontendAuthcConfigApi.DocumentLevel.GetAction.INSTANCE, FrontendAuthcConfigApi.DocumentLevel.GetAction.Handler.class));
            actions.add(new ActionHandler<>(FrontendAuthcConfigApi.DocumentLevel.PutAction.INSTANCE, FrontendAuthcConfigApi.DocumentLevel.PutAction.Handler.class));
            actions.add(new ActionHandler<>(FrontendAuthcConfigApi.DocumentLevel.PatchAction.INSTANCE, FrontendAuthcConfigApi.DocumentLevel.PatchAction.Handler.class));
            actions.add(new ActionHandler<>(SearchGuardCapabilities.GetCapabilitiesAction.INSTANCE, SearchGuardCapabilities.GetCapabilitiesAction.TransportAction.class));
            actions.add(new ActionHandler<>(ProtectedConfigIndexService.TriggerConfigIndexCreationAction.INSTANCE, ProtectedConfigIndexService.TriggerConfigIndexCreationAction.TransportAction.class));

            actions.add(new ActionHandler<>(MigrateConfigIndexApi.INSTANCE, MigrateConfigIndexApi.Handler.class));
        }

        actions.addAll(moduleRegistry.getActions());

        return actions;
    }

    @Override
    public List<ScriptContext<?>> getContexts() {
        return moduleRegistry.getContexts();
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        // called for every index!
        
        if (!disabled && !sslOnly) {
            if (adminDns == null) {
                throw new IllegalStateException("adminDns is not yet initialized");
            }

            ImmutableList<Function<IndexService, CheckedFunction<DirectoryReader, DirectoryReader, IOException>>> directoryReaderWrappersForNormalOperations = this.moduleRegistry
                    .getDirectoryReaderWrappersForNormalOperations();
            ImmutableList<Function<IndexService, CheckedFunction<DirectoryReader, DirectoryReader, IOException>>> directoryReaderWrappersForAllOperations = this.moduleRegistry
                    .getDirectoryReaderWrappersForAllOperations();
            
            indexModule.setReaderWrapper(
                    indexService -> new SearchGuardDirectoryReaderWrapper(indexService, adminDns, directoryReaderWrappersForNormalOperations, directoryReaderWrappersForAllOperations));
            
            ImmutableList<QueryCacheWeightProvider> queryCacheWeightProviders = moduleRegistry.getQueryCacheWeightProviders();
            
            if (!queryCacheWeightProviders.isEmpty()) {
         
                
                indexModule.forceQueryCacheProvider((indexSettings, nodeCache) -> new QueryCache() {

                    private Index index() {
                        return indexSettings.getIndex();
                    }

                    @Override
                    public void close() throws ElasticsearchException {
                        clear("close");
                    }

                    @Override
                    public void clear(String reason) {
                        nodeCache.clearIndex(index().getName());
                    }

                    @Override
                    public Weight doCache(Weight weight, QueryCachingPolicy policy) {
                        for (QueryCacheWeightProvider provider : queryCacheWeightProviders) {
                            Weight result = provider.apply(index(), weight, policy);

                            if (result != null) {
                                return result;
                            }
                        }

                        return nodeCache.doCache(weight, policy);
                    }
                });
            } 
            
            indexModule.addSearchOperationListener(new SearchOperationListener() {

                @Override
                public void onNewScrollContext(ReaderContext context) {
                    final boolean interClusterRequest = HeaderHelper.isInterClusterRequest(threadPool.getThreadContext());
                    if (Origin.LOCAL.toString().equals(threadPool.getThreadContext().getTransient(ConfigConstants.SG_ORIGIN))
                            && (interClusterRequest || HeaderHelper.isDirectRequest(threadPool.getThreadContext()))

                    ) {
                        context.putInContext("_sg_scroll_auth_local", Boolean.TRUE);

                    } else {
                        context.putInContext("_sg_scroll_auth", threadPool.getThreadContext().getTransient(ConfigConstants.SG_USER));
                    }
                }

                @Override
                public void validateReaderContext(ReaderContext context, TransportRequest transportRequest) {

                    final ScrollContext scrollContext = context.scrollContext();
                    if (scrollContext != null) {
                        final Object _isLocal = context.getFromContext("_sg_scroll_auth_local");
                        final Object _user = context.getFromContext("_sg_scroll_auth");
                        if (_user != null && (_user instanceof User)) {
                            final User scrollUser = (User) _user;
                            final User currentUser = threadPool.getThreadContext().getTransient(ConfigConstants.SG_USER);
                            if (!scrollUser.equals(currentUser)) {
                                auditLog.logMissingPrivileges(TransportSearchScrollAction.TYPE.name(), transportRequest, null);
                                log.error("Wrong user {} in scroll context, expected {}", scrollUser, currentUser);
                                throw new ElasticsearchSecurityException("Wrong user in scroll context", RestStatus.FORBIDDEN);
                            }
                        } else if (_isLocal != Boolean.TRUE) {
                            auditLog.logMissingPrivileges(TransportSearchScrollAction.TYPE.name(), transportRequest, null);
                            throw new ElasticsearchSecurityException("No user in scroll context", RestStatus.FORBIDDEN);
                        }
                    }
                }
            });
            
            for (SearchOperationListener searchOperationListener : this.moduleRegistry.getSearchOperationListeners()) {
                indexModule.addSearchOperationListener(searchOperationListener);
            }
            
            for (IndexingOperationListener indexOperationListener : this.moduleRegistry.getIndexOperationListeners()) {
                indexModule.addIndexOperationListener(indexOperationListener);
            }
        }
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        List<ActionFilter> filters = new ArrayList<>(1);
        if (!disabled && !sslOnly) {
            ResourceOwnerService resourceOwnerService = new ResourceOwnerService(localClient, clusterService, threadPool, protectedConfigIndexService,
                    evaluator, settings);
            ExtendedActionHandlingService extendedActionHandlingService = new ExtendedActionHandlingService(resourceOwnerService, settings);
            SearchGuardFilter searchGuardFilter = new SearchGuardFilter(authorizationService, evaluator, adminDns,
                    moduleRegistry.getSyncAuthorizationFilters(), moduleRegistry.getPrePrivilegeSyncAuthorizationFilters(), auditLog, threadPool,
                    clusterService, diagnosticContext, complianceConfig, actions, actionRequestIntrospector,
                    specialPrivilegesEvaluationContextProviderRegistry, extendedActionHandlingService, xContentRegistry);

            filters.add(searchGuardFilter);

            ActionFilter actionTraceFilter = diagnosticContext.getActionTraceFilter();

            if (actionTraceFilter != null) {
                filters.add(actionTraceFilter);
            }
            filters.addAll(moduleRegistry.getActionFilters());
        }
        return filters;
    }

    @Override
    public List<TransportInterceptor> getTransportInterceptors(NamedWriteableRegistry namedWriteableRegistry, ThreadContext threadContext) {
        List<TransportInterceptor> interceptors = new ArrayList<TransportInterceptor>(1);

        if (!disabled && !sslOnly) {
            interceptors.add(new TransportInterceptor() {

                @Override
                public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(String action, Executor executor,
                        boolean forceExecution, TransportRequestHandler<T> actualHandler) {

                    return (request, channel, task) -> sgi.getHandler(action, actualHandler).messageReceived(request, channel, task);

                }

                @Override
                public AsyncSender interceptSender(AsyncSender sender) {

                    return new AsyncSender() {

                        @Override
                        public <T extends TransportResponse> void sendRequest(Connection connection, String action, TransportRequest request,
                                TransportRequestOptions options, TransportResponseHandler<T> handler) {
                            sgi.sendRequestDecorate(sender, connection, action, request, options, handler);
                        }
                    };
                }
            });
        }

        return interceptors;
    }

    @Override
    public Map<String, Supplier<Transport>> getTransports(Settings settings, ThreadPool threadPool, PageCacheRecycler pageCacheRecycler,
            CircuitBreakerService circuitBreakerService, NamedWriteableRegistry namedWriteableRegistry, NetworkService networkService) {
        Map<String, Supplier<Transport>> transports = new HashMap<String, Supplier<Transport>>();

        if (sslOnly) {
            return super.getTransports(settings, threadPool, pageCacheRecycler, circuitBreakerService, namedWriteableRegistry, networkService);
        }

        if (transportSSLEnabled) {
            transports.put("com.floragunn.searchguard.ssl.http.netty.SearchGuardSSLNettyTransport",
                    () -> new SearchGuardSSLNettyTransport(settings, TransportVersion.current(), threadPool, networkService, pageCacheRecycler,
                            namedWriteableRegistry, circuitBreakerService, sharedGroupFactory, sgks, evaluateSslExceptionHandler()));
        }
        return transports;
    }

    @Override
    public Map<String, Supplier<HttpServerTransport>> getHttpTransports(Settings settings, ThreadPool threadPool, BigArrays bigArrays,
                                                                        PageCacheRecycler pageCacheRecycler, CircuitBreakerService circuitBreakerService, NamedXContentRegistry xContentRegistry,
                                                                        NetworkService networkService, Dispatcher dispatcher, BiConsumer<HttpPreRequest, ThreadContext> perRequestThreadContext, ClusterSettings clusterSettings, Tracer tracer) {

        if (sslOnly) {
            return super.getHttpTransports(settings, threadPool, bigArrays, pageCacheRecycler, circuitBreakerService, xContentRegistry,

                    networkService, dispatcher, perRequestThreadContext, clusterSettings, tracer);
        }

        Map<String, Supplier<HttpServerTransport>> httpTransports = new HashMap<String, Supplier<HttpServerTransport>>(1);

        if (!disabled) {
            if (httpSSLEnabled) {

                final ValidatingDispatcher validatingDispatcher = new ValidatingDispatcher(threadPool.getThreadContext(), dispatcher, settings,
                        configPath, evaluateSslExceptionHandler());
                //TODO close sghst
                final SearchGuardHttpServerTransport sghst = new SearchGuardHttpServerTransport(settings, networkService, threadPool, sgks,
                        evaluateSslExceptionHandler(), xContentRegistry, searchGuardRestFilter.wrap(validatingDispatcher), clusterSettings, sharedGroupFactory, tracer, perRequestThreadContext);

                httpTransports.put("com.floragunn.searchguard.http.SearchGuardHttpServerTransport", () -> sghst);
            } else {
                httpTransports.put("com.floragunn.searchguard.http.SearchGuardHttpServerTransport",
                        () -> new SearchGuardNonSslHttpServerTransport(settings, networkService, threadPool, xContentRegistry, searchGuardRestFilter.wrap(dispatcher),
                                perRequestThreadContext, clusterSettings, sharedGroupFactory, tracer));
            }
        }
        return httpTransports;
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        if (sslOnly) {
            return  super.createComponents(services);
        }

        this.threadPool = services.threadPool();
        this.xContentRegistry = services.xContentRegistry();
        this.clusterService = services.clusterService();
        this.localClient = services.client();
        this.scriptService = services.scriptService();

        final List<Object> components = new ArrayList<Object>();

        if (disabled) {
            return components;
        }

        GuiceDependencies guiceDependencies = new GuiceDependencies();
        components.add(guiceDependencies);

        final ClusterInfoHolder cih = new ClusterInfoHolder();
        this.clusterService.addListener(cih);

        actionRequestIntrospector = new ActionRequestIntrospector(
            services.indexNameExpressionResolver(),
            services.clusterService(), cih, guiceDependencies);

        final String DEFAULT_INTERCLUSTER_REQUEST_EVALUATOR_CLASS = DefaultInterClusterRequestEvaluator.class.getName();
        InterClusterRequestEvaluator interClusterRequestEvaluator = new DefaultInterClusterRequestEvaluator(settings);

        final String className = settings.get(ConfigConstants.SG_INTERCLUSTER_REQUEST_EVALUATOR_CLASS, DEFAULT_INTERCLUSTER_REQUEST_EVALUATOR_CLASS);
        log.debug("Using {} as intercluster request evaluator class", className);
        if (!DEFAULT_INTERCLUSTER_REQUEST_EVALUATOR_CLASS.equals(className)) {
            interClusterRequestEvaluator = ReflectionHelper.instantiateInterClusterRequestEvaluator(className, settings);
        }

        adminDns = new AdminDNs(settings);

        protectedConfigIndexService = new ProtectedConfigIndexService(services.client(),
            services.clusterService(), services.threadPool(), protectedIndices);
        moduleRegistry.addComponentStateProvider(protectedConfigIndexService);
        configVarService = new ConfigVarService(services.client(), services.clusterService(),
            services.threadPool(), protectedConfigIndexService, new EncryptionKeys(settings));
        moduleRegistry.addComponentStateProvider(configVarService);

        configModificationValidators = new ConfigModificationValidators();
        components.add(configModificationValidators);

        cr = new ConfigurationRepository(staticSettings,
            services.threadPool(), services.client(), services.clusterService(), configVarService, moduleRegistry, staticSgConfig,
            services.xContentRegistry(), services.environment(), services.indexNameExpressionResolver(), configModificationValidators);
        moduleRegistry.addComponentStateProvider(cr);

        licenseRepository = new LicenseRepository(settings, services.client(), services.clusterService(), cr);

        sslExceptionHandler = new AuditLogSslExceptionHandler(auditLog);

        complianceConfig = new ComplianceConfig(services.environment(), actionRequestIntrospector, cr);

        licenseRepository.subscribeOnLicenseChange(complianceConfig);
        moduleRegistry.addComponentStateProvider(licenseRepository);

        Actions actions = new Actions(moduleRegistry);

        this.authInfoService = new AuthInfoService(services.threadPool(), specialPrivilegesEvaluationContextProviderRegistry);
        this.authorizationService = new AuthorizationService(cr, staticSettings, authInfoService);
        evaluator = new PrivilegesEvaluator(
            services.clusterService(),
            services.threadPool(), cr, authorizationService, services.indexNameExpressionResolver(), auditLog, staticSettings, cih,
            actions, actionRequestIntrospector, specialPrivilegesEvaluationContextProviderRegistry, guiceDependencies,
            services.xContentRegistry(),
                enterpriseModulesEnabled);
        moduleRegistry.addComponentStateProvider(evaluator);

        InternalAuthTokenProvider internalAuthTokenProvider = new InternalAuthTokenProvider(authorizationService, evaluator, actions, cr);
        specialPrivilegesEvaluationContextProviderRegistry.add(internalAuthTokenProvider::userAuthFromToken);

        diagnosticContext = new DiagnosticContext(settings, services.threadPool().getThreadContext());

        InternalUsersDatabase internalUsersDatabase = new InternalUsersDatabase(cr);
        moduleRegistry.addComponentStateProvider(internalUsersDatabase);
        moduleRegistry.getTypedComponentRegistry().register(new InternalUsersAuthenticationBackend.Info(internalUsersDatabase));
        moduleRegistry.getTypedComponentRegistry().register(new InternalUsersAuthenticationBackend.UserInformationBackendInfo(internalUsersDatabase));

        final String principalExtractorClass = settings.get(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PRINCIPAL_EXTRACTOR_CLASS, null);

        if (principalExtractorClass == null) {
            principalExtractor = new com.floragunn.searchguard.ssl.transport.DefaultPrincipalExtractor();
        } else {
            principalExtractor = ReflectionHelper.instantiatePrincipalExtractor(principalExtractorClass);
        }

        BlockedIpRegistry blockedIpRegistry = new BlockedIpRegistry(cr);
        BlockedUserRegistry blockedUserRegistry = new BlockedUserRegistry(cr);

        BaseDependencies baseDependencies = new BaseDependencies(settings, staticSettings,
            services.client(),
            services.clusterService(),
            services.threadPool(),
            services.resourceWatcherService(),
            services.scriptService(),
            services.xContentRegistry(),
            services.environment(),
            services.nodeEnvironment(),
            services.indexNameExpressionResolver(), staticSgConfig, cr,
                licenseRepository, protectedConfigIndexService, internalAuthTokenProvider, specialPrivilegesEvaluationContextProviderRegistry, configVarService,
                diagnosticContext, auditLog, evaluator, blockedIpRegistry, blockedUserRegistry, moduleRegistry,
                internalUsersDatabase,
            actions, authorizationService, guiceDependencies, authInfoService, actionRequestIntrospector, services.featureService());

        sgi = new SearchGuardInterceptor(settings, services.threadPool(), auditLog, principalExtractor, interClusterRequestEvaluator,
            services.clusterService(),
                Objects.requireNonNull(sslExceptionHandler), Objects.requireNonNull(cih), guiceDependencies, diagnosticContext, adminDns);
        components.add(principalExtractor);
        components.add(adminDns);
        components.add(cr);
        components.add(evaluator);
        components.add(authorizationService);
        components.add(sgi);
        components.add(internalAuthTokenProvider);
        components.add(moduleRegistry);
        components.add(protectedConfigIndexService);
        components.add(staticSgConfig);
        components.add(authInfoService);
        components.add(diagnosticContext);
        components.add(configVarService);
        components.add(auditLog);
        components.add(licenseRepository);
        components.add(baseDependencies);

        Collection<Object> moduleComponents = moduleRegistry.createComponents(baseDependencies);

        components.addAll(moduleComponents);

        capabilities = new SearchGuardCapabilities(moduleRegistry.getModules(), services.clusterService(), services.client());
        components.add(capabilities);

        {
            AuditLog auditLog = moduleRegistry.getAuditLog();

            if (auditLog != null) {
                this.auditLog.setAuditLog(auditLog);
            }
        }

        searchGuardRestFilter = new AuthenticatingRestFilter(cr, moduleRegistry, adminDns, blockedIpRegistry, blockedUserRegistry, auditLog,
            services.threadPool(),
                principalExtractor, evaluator, settings, configPath, diagnosticContext);
        components.add(searchGuardRestFilter);

        evaluator.setMultiTenancyConfigurationProvider(moduleRegistry.getMultiTenancyConfigurationProvider());

        configModificationValidators.register(new RoleRelationsValidator(cr));
        configModificationValidators.register(moduleRegistry.getConfigModificationValidators());

        moduleRegistry.addComponentStateProvider(searchGuardRestFilter);

        this.actions = actions;

        return components;

    }

    @Override
    public Settings additionalSettings() {

        if (disabled) {
            return Settings.EMPTY;
        }

        final Settings.Builder builder = Settings.builder();

        builder.put(super.additionalSettings());

        if (!sslOnly) {
            builder.put(NetworkModule.TRANSPORT_TYPE_KEY, "com.floragunn.searchguard.ssl.http.netty.SearchGuardSSLNettyTransport");
            builder.put(NetworkModule.HTTP_TYPE_KEY, "com.floragunn.searchguard.http.SearchGuardHttpServerTransport");
        }
        return builder.build();
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<Setting<?>>();
        settings.addAll(super.getSettings());

        settings.add(Setting.boolSetting(ConfigConstants.SEARCHGUARD_SSL_ONLY, false, Property.NodeScope, Property.Filtered));

        if (!sslOnly) {
            settings.add(Setting.listSetting(ConfigConstants.SEARCHGUARD_AUTHCZ_ADMIN_DN, Collections.emptyList(), Function.identity(),
                    Property.NodeScope)); //not filtered here

            settings.add(Setting.simpleString(ConfigConstants.SEARCHGUARD_CERT_OID, Property.NodeScope, Property.Filtered));

            settings.add(Setting.simpleString(ConfigConstants.SEARCHGUARD_CERT_INTERCLUSTER_REQUEST_EVALUATOR_CLASS, Property.NodeScope,
                    Property.Filtered));
            settings.add(Setting.listSetting(ConfigConstants.SEARCHGUARD_NODES_DN, Collections.emptyList(), Function.identity(), Property.NodeScope));//not filtered here

            settings.add(Setting.boolSetting(ConfigConstants.SEARCHGUARD_DISABLED, false, Property.NodeScope, Property.Filtered));
           
            //SG6
            settings.add(Setting.boolSetting(ConfigConstants.SEARCHGUARD_ENTERPRISE_MODULES_ENABLED, true, Property.NodeScope, Property.Filtered));
            settings.add(
                    Setting.boolSetting(ConfigConstants.SEARCHGUARD_ALLOW_UNSAFE_DEMOCERTIFICATES, false, Property.NodeScope, Property.Filtered));
          
            settings.add(Setting.groupSetting(ConfigConstants.SEARCHGUARD_AUTHCZ_REST_IMPERSONATION_USERS + ".", Property.NodeScope)); //not filtered here

            // SG6 - Audit        
            settings.add(Setting.simpleString(ConfigConstants.SEARCHGUARD_AUDIT_TYPE_DEFAULT, Property.NodeScope, Property.Filtered));
            settings.add(Setting.groupSetting(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_ROUTES + ".", Property.NodeScope));
            settings.add(Setting.groupSetting(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_ENDPOINTS + ".", Property.NodeScope));
            settings.add(Setting.intSetting(ConfigConstants.SEARCHGUARD_AUDIT_THREADPOOL_SIZE, 10, Property.NodeScope, Property.Filtered));
            settings.add(Setting.intSetting(ConfigConstants.SEARCHGUARD_AUDIT_THREADPOOL_MAX_QUEUE_LEN, 100 * 1000, Property.NodeScope,
                    Property.Filtered));
            settings.add(Setting.boolSetting(ConfigConstants.SEARCHGUARD_AUDIT_LOG_REQUEST_BODY, true, Property.NodeScope, Property.Filtered));
            settings.add(Setting.boolSetting(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_INDICES, true, Property.NodeScope, Property.Filtered));
            settings.add(Setting.boolSetting(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_REST, true, Property.NodeScope, Property.Filtered));
            settings.add(Setting.boolSetting(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, true, Property.NodeScope, Property.Filtered));
            final List<String> disabledCategories = new ArrayList<String>(2);
            disabledCategories.add("AUTHENTICATED");
            disabledCategories.add("GRANTED_PRIVILEGES");
            settings.add(Setting.listSetting(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, disabledCategories,
                    Function.identity(), Property.NodeScope)); //not filtered here
            settings.add(Setting.listSetting(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, disabledCategories,
                    Function.identity(), Property.NodeScope)); //not filtered here
            final List<String> ignoredUsers = new ArrayList<String>(2);
            ignoredUsers.add("kibanaserver");
            settings.add(Setting.listSetting(ConfigConstants.SEARCHGUARD_AUDIT_IGNORE_USERS, ignoredUsers, Function.identity(), Property.NodeScope)); //not filtered here
            settings.add(Setting.listSetting(ConfigConstants.SEARCHGUARD_AUDIT_IGNORE_REQUESTS, Collections.emptyList(), Function.identity(),
                    Property.NodeScope)); //not filtered here
            settings.add(Setting.boolSetting(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, false, Property.NodeScope, Property.Filtered));
            settings.add(
                    Setting.boolSetting(ConfigConstants.SEARCHGUARD_AUDIT_EXCLUDE_SENSITIVE_HEADERS, true, Property.NodeScope, Property.Filtered));
            settings.add(Setting.listSetting(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_FIELDS,  Collections.emptyList(),
                Function.identity(), Property.NodeScope)); //not filtered here

            // SG6 - Audit - Sink
            settings.add(Setting.simpleString(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_ES_INDEX,
                    Property.NodeScope, Property.Filtered));

            settings.add(Setting.simpleString(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_ES_TYPE,
                    Property.NodeScope, Property.Filtered));
            settings.add(Setting.groupSetting(
                    ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_CUSTOM_ATTRIBUTES_PREFIX,
                    Property.NodeScope)
            );

            // External ES
            settings.add(Setting.listSetting(
                    ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_HTTP_ENDPOINTS,
                    Lists.newArrayList("localhost:9200"), Function.identity(), Property.NodeScope)); //not filtered here
            settings.add(Setting.simpleString(
                    ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_USERNAME,
                    Property.NodeScope, Property.Filtered));
            settings.add(Setting.simpleString(
                    ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_PASSWORD,
                    Property.NodeScope, Property.Filtered));
            settings.add(Setting.boolSetting(
                    ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_ENABLE_SSL, false,
                    Property.NodeScope, Property.Filtered));
            settings.add(Setting.boolSetting(
                    ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_VERIFY_HOSTNAMES, true,
                    Property.NodeScope, Property.Filtered));
            settings.add(Setting.boolSetting(
                    ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_ENABLE_SSL_CLIENT_AUTH,
                    false, Property.NodeScope, Property.Filtered));
            settings.add(Setting.simpleString(
                    ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_PEMCERT_CONTENT,
                    Property.NodeScope, Property.Filtered));
            settings.add(Setting.simpleString(
                    ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_PEMCERT_FILEPATH,
                    Property.NodeScope, Property.Filtered));
            settings.add(Setting.simpleString(
                    ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_PEMKEY_CONTENT,
                    Property.NodeScope, Property.Filtered));
            settings.add(Setting.simpleString(
                    ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_PEMKEY_FILEPATH,
                    Property.NodeScope, Property.Filtered));
            settings.add(Setting.simpleString(
                    ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_PEMKEY_PASSWORD,
                    Property.NodeScope, Property.Filtered));
            settings.add(Setting.simpleString(
                    ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_PEMTRUSTEDCAS_CONTENT,
                    Property.NodeScope, Property.Filtered));
            settings.add(Setting.simpleString(
                    ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_PEMTRUSTEDCAS_FILEPATH,
                    Property.NodeScope, Property.Filtered));
            settings.add(Setting.simpleString(
                    ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_JKS_CERT_ALIAS,
                    Property.NodeScope, Property.Filtered));
            settings.add(Setting.listSetting(
                    ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_ENABLED_SSL_CIPHERS,
                    Collections.emptyList(), Function.identity(), Property.NodeScope));//not filtered here
            settings.add(Setting.listSetting(
                    ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_ENABLED_SSL_PROTOCOLS,
                    Collections.emptyList(), Function.identity(), Property.NodeScope));//not filtered here

            // Webhooks
            settings.add(Setting.simpleString(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_WEBHOOK_URL,
                    Property.NodeScope, Property.Filtered));
            settings.add(
                    Setting.simpleString(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_WEBHOOK_FORMAT,
                            Property.NodeScope, Property.Filtered));
            settings.add(Setting.boolSetting(
                    ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_WEBHOOK_SSL_VERIFY, true,
                    Property.NodeScope, Property.Filtered));
            settings.add(Setting.simpleString(
                    ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_WEBHOOK_PEMTRUSTEDCAS_FILEPATH,
                    Property.NodeScope, Property.Filtered));
            settings.add(Setting.simpleString(
                    ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_WEBHOOK_PEMTRUSTEDCAS_CONTENT,
                    Property.NodeScope, Property.Filtered));

            // Log4j
            settings.add(Setting.simpleString(
                    ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_LOG4J_LOGGER_NAME, Property.NodeScope,
                    Property.Filtered));
            settings.add(Setting.simpleString(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_LOG4J_LEVEL,
                    Property.NodeScope, Property.Filtered));

            // Kerberos
            settings.add(Setting.simpleString(ConfigConstants.SEARCHGUARD_KERBEROS_KRB5_FILEPATH, Property.NodeScope, Property.Filtered));
            settings.add(Setting.simpleString(ConfigConstants.SEARCHGUARD_KERBEROS_ACCEPTOR_KEYTAB_FILEPATH, Property.NodeScope, Property.Filtered));
            settings.add(Setting.simpleString(ConfigConstants.SEARCHGUARD_KERBEROS_ACCEPTOR_PRINCIPAL, Property.NodeScope, Property.Filtered));

            // SG6 - REST API
            settings.add(Setting.listSetting(ConfigConstants.SEARCHGUARD_RESTAPI_ROLES_ENABLED, Collections.emptyList(), Function.identity(),
                    Property.NodeScope)); //not filtered here
            settings.add(Setting.groupSetting(ConfigConstants.SEARCHGUARD_RESTAPI_ENDPOINTS_DISABLED + ".", Property.NodeScope));
            settings.add(Setting.boolSetting(ConfigConstants.SEARCHGUARD_UNSUPPORTED_RESTAPI_ACCEPT_INVALID_LICENSE, false, Property.NodeScope,
                    Property.Filtered));

            settings.add(Setting.simpleString(ConfigConstants.SEARCHGUARD_RESTAPI_PASSWORD_VALIDATION_REGEX, Property.NodeScope, Property.Filtered));
            settings.add(Setting.simpleString(ConfigConstants.SEARCHGUARD_RESTAPI_PASSWORD_VALIDATION_ERROR_MESSAGE, Property.NodeScope,
                    Property.Filtered));

            // Compliance
            settings.add(Setting.listSetting(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_WRITE_WATCHED_INDICES, Collections.emptyList(),
                    Function.identity(), Property.NodeScope)); //not filtered here
            settings.add(Setting.listSetting(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_READ_WATCHED_FIELDS, Collections.emptyList(),
                    Function.identity(), Property.NodeScope)); //not filtered here
            settings.add(Setting.boolSetting(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_WRITE_METADATA_ONLY, false, Property.NodeScope,
                    Property.Filtered));
            settings.add(Setting.boolSetting(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_READ_METADATA_ONLY, false, Property.NodeScope,
                    Property.Filtered));
            settings.add(Setting.boolSetting(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_WRITE_LOG_DIFFS, false, Property.NodeScope,
                    Property.Filtered));
            settings.add(Setting.boolSetting(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_EXTERNAL_CONFIG_ENABLED, false, Property.NodeScope,
                    Property.Filtered));
            settings.add(Setting.boolSetting(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_EXTERNAL_CONFIG_ENV_VARS_ENABLED, true,
                    Property.NodeScope, Property.Filtered));
            settings.add(Setting.listSetting(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_READ_IGNORE_USERS, Collections.emptyList(),
                    Function.identity(), Property.NodeScope)); //not filtered here
            settings.add(Setting.listSetting(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_WRITE_IGNORE_USERS, Collections.emptyList(),
                    Function.identity(), Property.NodeScope)); //not filtered here
            settings.add(Setting.boolSetting(ConfigConstants.SEARCHGUARD_COMPLIANCE_DISABLE_ANONYMOUS_AUTHENTICATION, false, Property.NodeScope,
                    Property.Filtered));
            settings.add(Setting.listSetting(ConfigConstants.SEARCHGUARD_COMPLIANCE_IMMUTABLE_INDICES, Collections.emptyList(), Function.identity(),
                    Property.NodeScope)); //not filtered here
            settings.add(Setting.simpleString(ConfigConstants.SEARCHGUARD_COMPLIANCE_SALT, Property.NodeScope, Property.Filtered));
            settings.add(Setting.boolSetting(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_INTERNAL_CONFIG_ENABLED, false, Property.NodeScope,
                    Property.Filtered));
            settings.add(
                    Setting.boolSetting(ConfigConstants.SEARCHGUARD_COMPLIANCE_LOCAL_HASHING_ENABLED, false, Property.NodeScope, Property.Filtered));
            settings.add(Setting.simpleString(ConfigConstants.SEARCHGUARD_COMPLIANCE_MASK_PREFIX, Property.NodeScope, Property.Filtered));

            settings.add(Setting.listSetting(ConfigConstants.SEARCHGUARD_ALLOW_CUSTOM_HEADERS, Collections.emptyList(), Function.identity(),
                    Property.NodeScope));
            settings.add(Setting.boolSetting(ConfigConstants.SEARCHGUARD_DFM_EMPTY_OVERRIDES_ALL, false, Property.NodeScope, Property.Filtered, Property.Deprecated));
            
            // system integration
            settings.add(Setting.boolSetting(ConfigConstants.SEARCHGUARD_UNSUPPORTED_ALLOW_NOW_IN_DLS, false, Property.NodeScope, Property.Filtered));

            settings.add(Setting.boolSetting(ConfigConstants.SEARCHGUARD_UNSUPPORTED_RESTAPI_ALLOW_SGCONFIG_MODIFICATION, false, Property.NodeScope,
                    Property.Filtered));

            settings.add(
                    Setting.boolSetting(ConfigConstants.SEARCHGUARD_UNSUPPORTED_LOAD_STATIC_RESOURCES, true, Property.NodeScope, Property.Filtered));

            settings.add(Setting.simpleString(ConfigConstants.SEARCHGUARD_DLS_MODE, Property.NodeScope, Property.Filtered));

            settings.addAll(ResourceOwnerService.SUPPORTED_SETTINGS);

            settings.add(Setting.boolSetting(ConfigConstants.SEARCHGUARD_SSL_CERT_RELOAD_ENABLED, false, Property.NodeScope, Property.Filtered));
            settings.add(Setting.boolSetting(MultiTenancyChecker.SEARCHGUARD_MT_BOOTSTRAP_CHECK_ENABLED, false, Property.NodeScope, Property.Filtered));

            settings.add(SearchGuardModulesRegistry.DISABLED_MODULES);
            settings.add(EncryptionKeys.ENCRYPTION_KEYS_SETTING);
            settings.addAll(ConfigurationRepository.STATIC_SETTINGS.toPlatform());
            settings.addAll(moduleRegistry.getSettings());
            settings.addAll(DiagnosticContext.SETTINGS);
            settings.addAll(PrivilegesEvaluator.STATIC_SETTINGS.toPlatform());
            settings.addAll(AuthorizationService.STATIC_SETTINGS.toPlatform());

        }

        return settings;
    }

    @Override
    public List<BootstrapCheck> getBootstrapChecks() {
        List<BootstrapCheck> bootstrapChecks = new ArrayList<>(super.getBootstrapChecks());
        bootstrapChecks.add(new BootstrapCheck() {
            @Override
            public BootstrapCheckResult check(BootstrapContext context) {
                MultiTenancyChecker multiTenancyChecker = new MultiTenancyChecker(settings, new IndexRepository(context));
                Optional<String> errorDescription = multiTenancyChecker.findMultiTenancyConfigurationError();
                log.info("Multi-tenancy bootstrap check found errors '{}'", errorDescription);
                return errorDescription.map(BootstrapCheck.BootstrapCheckResult::failure)//
                    .orElseGet(BootstrapCheck.BootstrapCheckResult::success);
            }

            @Override
            public boolean alwaysEnforce() {
                // This is crucial line to execute this test in dev as well as prod environment
                // Please see org.elasticsearch.bootstrap.BootstrapChecks.check(org.elasticsearch.bootstrap.BootstrapContext, boolean, java.util.List<org.elasticsearch.bootstrap.BootstrapCheck>, org.apache.logging.log4j.Logger)
                return true;
            }

            @Override
            public ReferenceDocs referenceDocs() {
                return ReferenceDocs.BOOTSTRAP_CHECKS;
            }
        });
        log.info("SearchGuard plugin returned '{}' bootstrap checks", bootstrapChecks.size());
        return bootstrapChecks;
    }

    @Override
    public List<String> getSettingsFilter() {
        List<String> settingsFilter = new ArrayList<>();

        if (disabled) {
            return settingsFilter;
        }

        settingsFilter.add("searchguard.*");
        return settingsFilter;
    }

    @Override
    public void onNodeStarted() {
        log.info("Node started");
        if (!sslOnly && !disabled) {
            cr.initOnNodeStart();
            moduleRegistry.onNodeStarted();
            protectedConfigIndexService.onNodeStart();
        }
    }

    @Override
    public Function<String, FieldPredicate> getFieldFilter() {
        return (index) -> {
            ImmutableList<Function<String, FieldPredicate>> fieldFilters = this.moduleRegistry.getFieldFilters();

            List<FieldPredicate> predicates = fieldFilters
                    .stream().map(filter -> filter.apply(index))
                    .toList();

            return predicates.stream().reduce(FieldPredicate.ACCEPT_ALL, FieldPredicate.And::new);
        };
    }

    public static ProtectedIndices getProtectedIndices() {
        return Objects.requireNonNull(SearchGuardPlugin.protectedIndices);
    }

    public static final class ProtectedIndices {
        final Set<String> protectedPatterns;
        Pattern protectedPatternsPattern = Pattern.blank();
        
        public ProtectedIndices() {
            protectedPatterns = new HashSet<>();
        }

        private ProtectedIndices(Settings settings, String... patterns) {
            protectedPatterns = new HashSet<>();
            protectedPatterns.addAll(ConfigurationRepository.getConfiguredSearchguardIndices(settings));
            if (patterns != null && patterns.length > 0) {
                protectedPatterns.addAll(Arrays.asList(patterns));
            }
            try {
                protectedPatternsPattern = Pattern.createWithoutExclusions(protectedPatterns);
            } catch (ConfigValidationException e) {
                throw new RuntimeException("Invalid index pattern", e);
            }
        }

        public void add(String pattern) {
            protectedPatterns.add(pattern);
            try {
                protectedPatternsPattern = Pattern.createWithoutExclusions(protectedPatterns);
            } catch (ConfigValidationException e) {
                throw new RuntimeException("Invalid index pattern", e);
            }
        }

        public boolean isProtected(String index) {
            return protectedPatternsPattern.matches(index);
        }

        public boolean containsProtected(Collection<String> indices) {
            return protectedPatternsPattern.matches(indices);
        }

        public String printProtectedIndices() {
            return protectedPatterns == null ? "" : Joiner.on(',').join(protectedPatterns);
        }
    }
}
