package com.floragunn.signals;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.signals.proxy.service.HttpProxyHostRegistry;
import com.floragunn.signals.proxy.service.ProxyCrudService;
import com.floragunn.signals.proxy.service.persistence.ProxyData;
import com.floragunn.signals.proxy.service.persistence.ProxyRepository;
import com.floragunn.signals.truststore.service.TruststoreCrudService;
import com.floragunn.signals.truststore.service.TrustManagerRegistry;
import com.floragunn.signals.truststore.service.persistence.TruststoreData;
import com.floragunn.signals.truststore.service.persistence.TruststoreRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.configuration.ProtectedConfigIndexService;
import com.floragunn.searchguard.configuration.ProtectedConfigIndexService.ConfigIndex;
import com.floragunn.searchguard.configuration.ProtectedConfigIndexService.FailureListener;
import com.floragunn.searchguard.internalauthtoken.InternalAuthTokenProvider;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentState.State;
import com.floragunn.searchsupport.diag.DiagnosticContext;
import com.floragunn.signals.accounts.AccountRegistry;
import com.floragunn.signals.settings.SignalsSettings;
import com.floragunn.signals.settings.SignalsSettings.SignalsStaticSettings.IndexNames;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.state.WatchState;
import com.google.common.collect.ImmutableList;
import com.google.common.io.BaseEncoding;

public class Signals extends AbstractLifecycleComponent {
    private static final Logger log = LogManager.getLogger(Signals.class);

    private final ComponentState componentState;
    private final SignalsSettings signalsSettings;
    private NodeEnvironment nodeEnvironment;

    private final Map<String, SignalsTenant> tenants = new ConcurrentHashMap<>();
    private Set<String> configuredTenants;
    private Client client;
    private ClusterService clusterService;
    private NamedXContentRegistry xContentRegistry;
    private ScriptService scriptService;
    private InternalAuthTokenProvider internalAuthTokenProvider;
    private AccountRegistry accountRegistry;
    private InitializationState initState = InitializationState.INITIALIZING;
    private Exception initException;
    private Settings settings;
    private String nodeId;
    private Map<String, Exception> tenantInitErrors = new ConcurrentHashMap<>();
    private DiagnosticContext diagnosticContext;
    private ThreadPool threadPool;

    private TrustManagerRegistry trustManagerRegistry;
    private HttpProxyHostRegistry httpProxyHostRegistry;

    public Signals(Settings settings, ComponentState componentState) {
        this.componentState = componentState;
        this.settings = settings;
        this.signalsSettings = new SignalsSettings(settings);
        this.signalsSettings.addChangeListener(this.settingsChangeListener);
    }

    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService, ScriptService scriptService, NamedXContentRegistry xContentRegistry,
            Environment environment, NodeEnvironment nodeEnvironment, InternalAuthTokenProvider internalAuthTokenProvider,
            ProtectedConfigIndexService protectedConfigIndexService, DiagnosticContext diagnosticContext) {

        try {
            nodeId = nodeEnvironment.nodeId();

            if (!signalsSettings.getStaticSettings().isEnabled()) {
                initState = InitializationState.DISABLED;
                return Collections.emptyList();
            }

            this.client = client;
            this.clusterService = clusterService;
            this.threadPool = threadPool;
            this.nodeEnvironment = nodeEnvironment;
            this.xContentRegistry = xContentRegistry;
            this.scriptService = scriptService;
            this.internalAuthTokenProvider = internalAuthTokenProvider;
            this.diagnosticContext = diagnosticContext;

            createIndexes(protectedConfigIndexService);

            if (settings.getAsBoolean(ConfigConstants.SEARCHGUARD_ENTERPRISE_MODULES_ENABLED, true)
                    && signalsSettings.getStaticSettings().isEnterpriseEnabled()) {
                initEnterpriseModules();
            }

            this.accountRegistry = new AccountRegistry(signalsSettings);
            PrivilegedConfigClient privilegedConfigClient = PrivilegedConfigClient.adapt(client);
            TruststoreRepository truststoreRepository = new TruststoreRepository(signalsSettings, privilegedConfigClient);
            TruststoreCrudService truststoreCrudService = new TruststoreCrudService(truststoreRepository);
            this.trustManagerRegistry = new TrustManagerRegistry(truststoreCrudService);

            ProxyRepository proxyRepository = new ProxyRepository(signalsSettings, privilegedConfigClient);
            ProxyCrudService proxyCrudService = new ProxyCrudService(proxyRepository);
            this.httpProxyHostRegistry = new HttpProxyHostRegistry(proxyCrudService);
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

    private void createIndexes(ProtectedConfigIndexService protectedConfigIndexService) {

        IndexNames indexNames = signalsSettings.getStaticSettings().getIndexNames();

        String[] allIndexes = new String[] { indexNames.getWatches(), indexNames.getWatchesState(), indexNames.getWatchesTriggerState(),
                indexNames.getAccounts(), indexNames.getSettings(), IndexNames.TRUSTSTORES, IndexNames.PROXIES };

        componentState.addPart(protectedConfigIndexService.createIndex(new ConfigIndex(indexNames.getWatches()).mapping(Watch.getIndexMapping(), 2)
                .mappingUpdate(0, Watch.getIndexMappingUpdate()).dependsOnIndices(allIndexes).onIndexReady(this::init)));
        ConfigIndex truststoreConfigIndex = new ConfigIndex(IndexNames.TRUSTSTORES).mapping(TruststoreData.MAPPINGS);
        componentState.addPart(protectedConfigIndexService.createIndex(truststoreConfigIndex));
        ConfigIndex proxyConfigIndex = new ConfigIndex(IndexNames.PROXIES).mapping(ProxyData.MAPPINGS);
        componentState.addPart(protectedConfigIndexService.createIndex(proxyConfigIndex));
        componentState.addPart(
                protectedConfigIndexService.createIndex(new ConfigIndex(indexNames.getWatchesState()).mapping(WatchState.getIndexMapping())));
        componentState.addPart(protectedConfigIndexService.createIndex(new ConfigIndex(indexNames.getWatchesTriggerState())));
        componentState.addPart(protectedConfigIndexService.createIndex(new ConfigIndex(indexNames.getAccounts())));
        componentState.addPart(protectedConfigIndexService.createIndex(new ConfigIndex(indexNames.getSettings())));
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

    private void createTenant(String name) {
        if ("SGS_GLOBAL_TENANT".equals(name)) {
            name = "_main";
        }

        ComponentState tenantState = componentState.getOrCreatePart("tenant", name);
        tenantState.setMandatory(false);

        try {

            SignalsTenant signalsTenant = SignalsTenant.create(name, client, clusterService, nodeEnvironment, scriptService, xContentRegistry,
                    internalAuthTokenProvider, signalsSettings, accountRegistry, tenantState, diagnosticContext, threadPool,
                    trustManagerRegistry, httpProxyHostRegistry);

            tenants.put(name, signalsTenant);

            log.debug("Tenant {} created", name);
        } catch (Exception e) {
            log.error("Error while creating tenant " + name, e);
            tenantInitErrors.put(name, e);
            tenantState.setFailed(e);
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

    private synchronized void init(FailureListener failureListener) {
        if (initState == InitializationState.INITIALIZED) {
            return;
        }

        try {
            log.info("Initializing Signals");

            componentState.setState(State.INITIALIZING, "reading_settings");
            signalsSettings.refresh(client);

            componentState.setState(State.INITIALIZING, "reading_accounts");
            accountRegistry.init(client);
            loadAllTruststores();
            loadAllProxies();

            componentState.setState(State.INITIALIZING, "initializing_keys");
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

            createSignalsLogIndex();

            componentState.setState(State.INITIALIZING, "creating_tenants");

            if (configuredTenants != null) {
                log.debug("Initializing tenant schedulers");

                for (String tenant : configuredTenants) {
                    createTenant(tenant);
                }
            }

            failureListener.onSuccess();

            initState = InitializationState.INITIALIZED;
            componentState.setInitialized();
            
            log.info("Signals has been successfully initialized");
            
        } catch (SignalsInitializationException e) {
            failureListener.onFailure(e);
            initState = InitializationState.FAILED;
            initException = e;
            componentState.setFailed(e);
        }
    }

    private void createSignalsLogIndex() {
        String signalsLogIndex = signalsSettings.getDynamicSettings().getWatchLogIndex();

        if (!clusterService.state().nodes().isLocalNodeElectedMaster()) {
            log.debug("Not checking signals_log index because local node is not master");
            return;
        }

        if (clusterService.state().getMetadata().componentTemplates().containsKey("signals_log_template")) {
            log.debug("Template signals_log_template does already exist.");
            return;
        }

        if (signalsLogIndex.startsWith("<") && signalsLogIndex.endsWith(">")) {
            signalsLogIndex = signalsLogIndex.substring(1, signalsLogIndex.length() - 1).replaceAll("\\{.*\\}", "*");
        }

        if (!signalsLogIndex.startsWith(".")) {
            log.debug("signals log index does not start with ., so we do not need to create a template");
            return;
        }

        log.debug("Creating signals_log_template for {}", signalsLogIndex);

        PutComposableIndexTemplateAction.Request putRequest = new PutComposableIndexTemplateAction.Request("signals_log_template");
        putRequest.indexTemplate(new ComposableIndexTemplate(
            ImmutableList.of(signalsLogIndex),
            new Template(Settings.builder().put("index.hidden", true).build(), null, null),
            null,
            null,
            null,
            null));

        client.execute(PutComposableIndexTemplateAction.INSTANCE, putRequest, new ActionListener<AcknowledgedResponse>() {

            @Override public void onResponse(AcknowledgedResponse response) {
                log.debug("Created signals_log_template");
            }

            @Override public void onFailure(Exception e) {
                componentState.addLastException("create_signals_log_template", e);
                log.error("Error while creating signals_log_template", e);
            }

        });
    }

    private void loadAllTruststores() throws SignalsInitializationException {
        try {
            trustManagerRegistry.reloadAll();
        } catch (Exception e) {
            throw new SignalsInitializationException("Cannot load all trust stores.", e);
        }
    }

    private void loadAllProxies() throws SignalsInitializationException {
        try {
            httpProxyHostRegistry.reloadAll();
        } catch (Exception e) {
            throw new SignalsInitializationException("Cannot load all http proxies.", e);
        }
    }

    synchronized void updateTenants(Set<String> configuredTenants) {
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
                createTenant(tenantToBeCreated);
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

    public TrustManagerRegistry getTruststoreRegistry() {
        return trustManagerRegistry;
    }
    public HttpProxyHostRegistry getHttpProxyHostRegistry() {
        return httpProxyHostRegistry;
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

    public enum InitializationState {
        INITIALIZING, INITIALIZED, FAILED, DISABLED
    }
}
