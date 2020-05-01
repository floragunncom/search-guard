package com.floragunn.signals;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import com.floragunn.searchguard.SearchGuardPlugin.ProtectedIndices;
import com.floragunn.searchguard.internalauthtoken.InternalAuthTokenProvider;
import com.floragunn.searchguard.sgconf.ConfigModel;
import com.floragunn.searchguard.sgconf.DynamicConfigFactory;
import com.floragunn.searchguard.sgconf.DynamicConfigFactory.DCFListener;
import com.floragunn.searchguard.sgconf.DynamicConfigModel;
import com.floragunn.searchguard.sgconf.InternalUsersModel;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.jobs.actions.SchedulerActions;
import com.floragunn.searchsupport.jobs.config.validation.ConfigValidationException;
import com.floragunn.signals.accounts.AccountRegistry;
import com.floragunn.signals.actions.SignalsActions;
import com.floragunn.signals.script.SignalsScriptContexts;
import com.floragunn.signals.settings.SignalsSettings;
import com.google.common.io.BaseEncoding;

public class Signals extends AbstractLifecycleComponent {
    private static final Logger log = LogManager.getLogger(Signals.class);

    private final SignalsSettings signalsSettings;
    private SignalsIndexes signalsIndexes;
    private final Map<String, SignalsTenant> tenants = new ConcurrentHashMap<>();
    private Set<String> configuredTenants;
    private Client client;
    private ClusterService clusterService;
    private NodeEnvironment nodeEnvironment;
    private NamedXContentRegistry xContentRegistry;
    private ScriptService scriptService;
    private InternalAuthTokenProvider internalAuthTokenProvider;
    private AccountRegistry accountRegistry;
    private InitializationState initState = InitializationState.INITIALIZING;
    private Exception initException;
    private ThreadPool threadPool;
    private Settings settings;
    private String nodeId;
    private Map<String, Exception> tenantInitErrors = new ConcurrentHashMap<>();

    public Signals(Settings settings, Path configPath) {
        this.settings = settings;
        this.signalsSettings = new SignalsSettings(settings);
        this.signalsSettings.addChangeListener(this.settingsChangeListener);
    }

    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService, ScriptService scriptService, NamedXContentRegistry xContentRegistry,
            Environment environment, NodeEnvironment nodeEnvironment, InternalAuthTokenProvider internalAuthTokenProvider,
            ProtectedIndices protectedIndices, NamedWriteableRegistry namedWriteableRegistry, DynamicConfigFactory dynamicConfigFactory) {

        try {
            nodeId = nodeEnvironment.nodeId();

            if (!signalsSettings.getStaticSettings().isEnabled()) {
                initState = InitializationState.DISABLED;
                return Collections.emptyList();
            }

            this.client = client;
            this.clusterService = clusterService;
            this.nodeEnvironment = nodeEnvironment;
            this.xContentRegistry = xContentRegistry;
            this.scriptService = scriptService;
            this.internalAuthTokenProvider = internalAuthTokenProvider;

            this.signalsIndexes = new SignalsIndexes(this, signalsSettings, client);
            this.signalsIndexes.protectIndexes(protectedIndices);
            clusterService.addListener(this.signalsIndexes.getClusterStateListener());

            if (settings.getAsBoolean(ConfigConstants.SEARCHGUARD_ENTERPRISE_MODULES_ENABLED, true)
                    && signalsSettings.getStaticSettings().isEnterpriseEnabled()) {
                initEnterpriseModules();
            }

            this.accountRegistry = new AccountRegistry(signalsSettings);
            this.threadPool = threadPool;

            dynamicConfigFactory.registerDCFListener(dcfListener);

            return Collections.singletonList(this);

        } catch (Exception e) {
            initState = InitializationState.FAILED;
            initException = e;
            log.error("Error while initializing Signals", e);
            throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
        }
    }

    public SignalsTenant getTenant(User user) throws SignalsUnavailableException, NoSuchTenantException {
        if (user == null) {
            throw new IllegalArgumentException("No user specified");
        } else {
            return getTenant(user.getRequestedTenant());
        }
    }

    public SignalsTenant getTenant(String name) throws SignalsUnavailableException, NoSuchTenantException {
        checkInitState();

        if (name == null || name.length() == 0 || "_main".equals(name) || "SGS_GLOBAL_TENANT".equals(name)) {
            name = "_main";
        }

        SignalsTenant result = this.tenants.get(name);

        if (result != null) {
            return result;
        } else {
            Exception tenantInitError = tenantInitErrors.get(name);

            if (tenantInitError != null) {
                throw new SignalsUnavailableException("Tenant " + name + " failed to intialize", nodeId, null, tenantInitError);
            } else {
                throw new NoSuchTenantException(name);
            }
        }
    }

    public static List<Setting<?>> getSettings() {
        return SignalsSettings.StaticSettings.getAvailableSettings();
    }

    public static List<ScriptContext<?>> getContexts() {
        return SignalsScriptContexts.CONTEXTS;
    }

    public static List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> result = new ArrayList<>(SchedulerActions.getActions());
        result.addAll(SignalsActions.getActions());
        return result;
    }

    private void checkInitState() throws SignalsUnavailableException {
        switch (initState) {
        case INITIALIZED:
            return;
        case DISABLED:
            throw new SignalsUnavailableException("Signals is disabled", nodeId, initState);
        case INITIALIZING:
            if (initException != null) {
                throw new SignalsUnavailableException(
                        "Signals encountered errors while initializing but is still trying to start up. Please try again later.", nodeId, initState,
                        initException);
            } else {
                throw new SignalsUnavailableException("Signals is still initializing. Please try again later.", nodeId, initState);
            }
        case FAILED:
            throw new SignalsUnavailableException("Signals failed to initialize on node " + nodeId + ". Please contact admin or check the ES logs.",
                    nodeId, initState, initException);
        }
    }

    private void waitForYellowStatus() throws InterruptedException {
        SignalsSettings.StaticSettings.IndexNames indexNames = signalsSettings.getStaticSettings().getIndexNames();
        long start = System.currentTimeMillis();
        Exception lastException = null;

        do {
            try {
                ClusterHealthResponse clusterHealthResponse = client.admin().cluster().health(new ClusterHealthRequest(indexNames.getWatches(),
                        indexNames.getWatchesState(), indexNames.getSettings(), indexNames.getAccounts()).waitForYellowStatus()).actionGet();

                if (log.isDebugEnabled()) {
                    log.debug("chr: " + clusterHealthResponse);
                }

                if (clusterHealthResponse.getStatus() == ClusterHealthStatus.YELLOW
                        || clusterHealthResponse.getStatus() == ClusterHealthStatus.GREEN) {
                    return;
                } else {
                    if (System.currentTimeMillis() - start > 30 * 1000) {
                        Exception indexInitException = signalsIndexes.getInitException();

                        if (indexInitException != null) {
                            initException = new Exception("Cluster is still in RED state; " + indexInitException.getMessage(), indexInitException);
                        } else {
                            initException = new Exception("Cluster is still in RED state");
                        }
                    }

                    Thread.sleep(500);
                }
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                if (log.isDebugEnabled()) {
                    log.debug("Error while waiting for cluster health", e);
                }

                if (System.currentTimeMillis() - start > 30 * 1000) {
                    Exception indexInitException = signalsIndexes.getInitException();

                    if (indexInitException != null) {
                        initException = new Exception("Cluster is still in RED state; " + indexInitException.getMessage(), indexInitException);
                    } else {
                        initException = new Exception("Cluster is still in RED state");
                    }
                }

                lastException = e;

                Thread.sleep(500);
            }
        } while (System.currentTimeMillis() < start + 60 * 60 * 1000);

        throw new RuntimeException("Giving up waiting for YELLOW status after 60 minutes of trying. Don't say that I did not wait long enough! ^^",
                lastException);

    }

    private void createTenant(String name) throws SignalsInitializationException {
        try {
            if ("SGS_GLOBAL_TENANT".equals(name)) {
                name = "_main";
            }

            SignalsTenant signalsTenant = SignalsTenant.create(name, client, clusterService, scriptService, xContentRegistry, nodeEnvironment,
                    internalAuthTokenProvider, signalsSettings, accountRegistry);

            tenants.put(name, signalsTenant);

            log.debug("Tenant {} created", name);
        } catch (Exception e) {
            throw new SignalsInitializationException("Error while creating tenant " + name, e);
        }
    }

    private void deleteTenant(String name) throws SignalsUnavailableException, NoSuchTenantException {
        SignalsTenant tenant = getTenant(name);
        if (tenant != null) {
            tenant.delete();
            tenants.remove(name);
            log.debug("Tenant {} deleted", name);
        } else {
            log.debug("Trying to delete non-existing tenant {}", name);
        }
    }

    private synchronized void init() {
        if (initState == InitializationState.INITIALIZED) {
            return;
        }

        try {
            log.info("Initializing Signals");

            signalsSettings.refresh(client);

            accountRegistry.init(client);

            if (signalsSettings.getDynamicSettings().getInternalAuthTokenSigningKey() != null) {
                internalAuthTokenProvider.setSigningKey(signalsSettings.getDynamicSettings().getInternalAuthTokenSigningKey());
            }

            if (signalsSettings.getDynamicSettings().getInternalAuthTokenEncryptionKey() != null) {
                internalAuthTokenProvider.setEncryptionKey(signalsSettings.getDynamicSettings().getInternalAuthTokenEncryptionKey());
            }

            if ((signalsSettings.getDynamicSettings().getInternalAuthTokenSigningKey() == null
                    || signalsSettings.getDynamicSettings().getInternalAuthTokenEncryptionKey() == null)
                    && clusterService.state().nodes().isLocalNodeElectedMaster()) {
                log.info("Generating keys for internal auth token");

                String signingKey = signalsSettings.getDynamicSettings().getInternalAuthTokenSigningKey();
                String encryptionKey = signalsSettings.getDynamicSettings().getInternalAuthTokenEncryptionKey();

                if (signingKey == null) {
                    signingKey = generateKey(512);
                }

                if (encryptionKey == null) {
                    encryptionKey = generateKey(256);
                }

                try {
                    signalsSettings.getDynamicSettings().update(client, SignalsSettings.DynamicSettings.INTERNAL_AUTH_TOKEN_SIGNING_KEY.getKey(),
                            signingKey, SignalsSettings.DynamicSettings.INTERNAL_AUTH_TOKEN_ENCRYPTION_KEY.getKey(), encryptionKey);
                } catch (ConfigValidationException e) {
                    log.error("Could not init encryption keys. This should not happen", e);
                    throw new SignalsInitializationException("Could not init encryption keys. This should not happen", e);
                }
            }

            if (configuredTenants != null) {
                log.debug("Initializing tenant schedulers");

                for (String tenant : configuredTenants) {
                    try {
                        createTenant(tenant);
                    } catch (Exception e) {
                        tenantInitErrors.put(tenant, e);
                    }
                }
            }

            initState = InitializationState.INITIALIZED;
        } catch (SignalsInitializationException e) {
            initState = InitializationState.FAILED;
            initException = e;
        }
    }

    private synchronized void updateTenants(Set<String> configuredTenants) {
        configuredTenants = new HashSet<>(configuredTenants);

        // ensure we always have a default tenant
        configuredTenants.add("_main");
        configuredTenants.remove("SGS_GLOBAL_TENANT");

        if (initState == InitializationState.INITIALIZED) {
            Set<String> currentTenants = this.tenants.keySet();

            for (String tenantToBeDeleted : Sets.difference(currentTenants, configuredTenants)) {
                try {
                    deleteTenant(tenantToBeDeleted);
                } catch (NoSuchTenantException e) {
                    log.debug("Tenant to be deleted does not exist", e);
                } catch (Exception e) {
                    log.error("Error while deleting tenant", e);
                }
            }

            for (String tenantToBeCreated : Sets.difference(configuredTenants, currentTenants)) {
                try {
                    createTenant(tenantToBeCreated);
                } catch (Exception e) {
                    log.error("Error while creating tenant", e);
                    tenantInitErrors.put(tenantToBeCreated, e);
                }
            }
        } else {
            this.configuredTenants = configuredTenants;
        }
    }

    private String generateKey(int bits) {
        SecureRandom random = new SecureRandom();
        byte bytes[] = new byte[bits / 8];
        random.nextBytes(bytes);
        return BaseEncoding.base64().encode(bytes);
    }

    private void initEnterpriseModules() throws SignalsInitializationException {
        Class<?> signalsEnterpriseFeatures;

        try {

            signalsEnterpriseFeatures = Class.forName("com.floragunn.signals.enterprise.SignalsEnterpriseFeatures");

        } catch (ClassNotFoundException e) {
            throw new SignalsInitializationException("Signals enterprise features not found", e);
        }

        try {
            signalsEnterpriseFeatures.getDeclaredMethod("init").invoke(null);
        } catch (InvocationTargetException e) {
            throw new SignalsInitializationException("Error while initializing Signals enterprise features", e.getTargetException());
        } catch (IllegalAccessException | IllegalArgumentException | NoSuchMethodException | SecurityException e) {
            throw new SignalsInitializationException("Error while initializing Signals enterprise features", e);
        }
    }

    @Override
    protected void doStart() {
        this.threadPool.generic().submit(() -> {
            try {
                waitForYellowStatus();
                init();
            } catch (Exception e) {
                log.error("Error while starting Signals", e);
            }
        });
    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() throws IOException {

    }

    public AccountRegistry getAccountRegistry() {
        return accountRegistry;
    }

    public SignalsSettings getSignalsSettings() {
        return signalsSettings;
    }

    synchronized void setInitException(Exception e) {
        if (initException != null) {
            return;
        }

        initException = e;
        initState = InitializationState.FAILED;
    }

    private final SignalsSettings.ChangeListener settingsChangeListener = new SignalsSettings.ChangeListener() {

        @Override
        public void onChange() {
            internalAuthTokenProvider.setSigningKey(signalsSettings.getDynamicSettings().getInternalAuthTokenSigningKey());
            internalAuthTokenProvider.setEncryptionKey(signalsSettings.getDynamicSettings().getInternalAuthTokenEncryptionKey());
        }
    };

    private final DCFListener dcfListener = new DCFListener() {
        @Override
        public void onChanged(ConfigModel cm, DynamicConfigModel dcm, InternalUsersModel ium) {
            log.debug("Tenant config model changed");
            updateTenants(cm.getAllConfiguredTenantNames());
        }
    };

    public enum InitializationState {
        INITIALIZING, INITIALIZED, FAILED, DISABLED
    }
}
