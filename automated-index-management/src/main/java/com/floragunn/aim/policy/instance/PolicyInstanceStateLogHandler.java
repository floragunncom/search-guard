package com.floragunn.aim.policy.instance;

import com.floragunn.aim.AutomatedIndexManagementSettings;
import com.floragunn.aim.api.internal.InternalPolicyAPI;
import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.PolicyService;
import com.floragunn.aim.policy.actions.Action;
import com.floragunn.aim.policy.conditions.Condition;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.RestStatus;

import java.time.Instant;

public class PolicyInstanceStateLogHandler {
    private static final Logger LOG = LogManager.getLogger(PolicyInstanceStateLogHandler.class);

    public static String getWriteAliasName(String aliasName) {
        return aliasName + "-write";
    }

    private final AutomatedIndexManagementSettings settings;
    private final Client client;
    private final PolicyService policyService;
    private final PolicyInstanceService policyInstanceService;
    private final Condition.Factory conditionFactory;
    private final Action.Factory actionFactory;

    private volatile boolean initialized = false;

    public PolicyInstanceStateLogHandler(AutomatedIndexManagementSettings settings, Client client, PolicyService policyService,
            PolicyInstanceService policyInstanceService, Condition.Factory conditionFactory, Action.Factory actionFactory) {
        this.settings = settings;
        this.client = client;
        this.policyService = policyService;
        this.policyInstanceService = policyInstanceService;
        this.conditionFactory = conditionFactory;
        this.actionFactory = actionFactory;
    }

    public synchronized void init(StateLogReadyListener listener) {
        try {
            setupPolicy();
            setupIndexTemplate();
            setupIndex();
            initialized = true;
            policyInstanceService.setPolicyInstanceStateLogHandler(this);
            listener.onLogReady();
        } catch (Exception e) {
            listener.onLogFailure(e);
        }
    }

    public synchronized void stop() {
        policyInstanceService.setPolicyInstanceStateLogHandler(null);
        initialized = false;
    }

    private void setupPolicy() throws StateLogInitializationException {
        String policyName = settings.getStatic().stateLog().getPolicyName();
        try {
            GetResponse getResponse = policyService.getPolicy(policyName);
            if (getResponse.isExists()) {
                LOG.debug("State log policy already exists. Skipping creation");
                return;
            }
            DocNode policyNode = DocNode.parse(Format.JSON)
                    .from(PolicyInstanceStateLogHandler.class.getResourceAsStream("/policies/state_log_policy.json"));
            Policy policy = Policy.parse(policyNode, Policy.ParsingContext.strict(conditionFactory, actionFactory));
            InternalPolicyAPI.StatusResponse response = policyService.putPolicy(policyName, policy);
            if (response.status() != RestStatus.CREATED) {
                throw new IllegalStateException("Unexpected response status on policy create: " + response.status().name());
            }
        } catch (ConfigValidationException e) {
            throw new StateLogInitializationException("Failed to setup state log policy. Policy was invalid: " + e.toPrettyJsonString());
        } catch (Exception e) {
            throw new StateLogInitializationException("Failed to setup state log policy", e);
        }
    }

    private void setupIndexTemplate() throws StateLogInitializationException {
        String indexTemplateName = settings.getStatic().stateLog().getIndexTemplateName();
        String indexNamePrefix = settings.getStatic().stateLog().getIndexNamePrefix();
        String aliasName = settings.getStatic().stateLog().getAliasName();
        String writeAliasName = getWriteAliasName(aliasName);
        String policyName = settings.getStatic().stateLog().getPolicyName();
        try {
            GetIndexTemplatesResponse getIndexTemplatesResponse = client.admin().indices().prepareGetTemplates(indexTemplateName).get();
            if (!getIndexTemplatesResponse.getIndexTemplates().isEmpty()) {
                LOG.debug("State log index template already exists. Skipping creation");
                return;
            }
            AcknowledgedResponse putTemplateResponse = client.admin().indices().preparePutTemplate(indexTemplateName).setCreate(true)
                    .setPatterns(ImmutableList.of(indexNamePrefix + "*")).addAlias(new Alias(aliasName).isHidden(true).writeIndex(false))
                    .setSettings(Settings.builder().put("index.hidden", true)
                            .put(AutomatedIndexManagementSettings.Static.POLICY_NAME_FIELD.name(), policyName)
                            .put(AutomatedIndexManagementSettings.Static.ALIASES_FIELD.name() + "."
                                    + AutomatedIndexManagementSettings.Static.DEFAULT_ROLLOVER_ALIAS_KEY, writeAliasName)
                            .put(AutomatedIndexManagementSettings.Static.ALIASES_FIELD.name() + ".read_alias", aliasName))
                    .get();
            if (!putTemplateResponse.isAcknowledged()) {
                throw new StateLogInitializationException("Failed to create state log index template. Response was not acknowledged");
            }
        } catch (StateLogInitializationException e) {
            throw e;
        } catch (Exception e) {
            throw new StateLogInitializationException("Failed to setup state log index template", e);
        }
    }

    private void setupIndex() throws StateLogInitializationException {
        String aliasName = settings.getStatic().stateLog().getAliasName();
        try {
            try {
                client.admin().indices().getIndex(new GetIndexRequest().indices(aliasName)).actionGet();
                LOG.debug("State log alias already exists. Skipping index creation");
                return;
            } catch (IndexNotFoundException ignored) {
            }
            String indexNamePrefix = settings.getStatic().stateLog().getIndexNamePrefix();
            String indexName = indexNamePrefix + "-000001";
            CreateIndexResponse indexResponse = client.admin().indices().prepareCreate(indexName)
                    .addAlias(new Alias(aliasName + "-write").isHidden(true).writeIndex(true)).execute().actionGet();
            if (!indexResponse.isAcknowledged()) {
                throw new StateLogInitializationException("Failed to setup state log index. Response was not acknowledged");
            } else {
                LOG.debug("Initialized state log index '{}'", indexResponse.index());
            }
        } catch (StateLogInitializationException e) {
            throw e;
        } catch (Exception e) {
            throw new StateLogInitializationException("Failed to setup state log index", e);
        }
    }

    public void putStateLogEntry(String index, PolicyInstanceState state) {
        if (initialized && settings.getDynamic().getStateLogActive()) {
            if (PolicyInstanceState.Status.WAITING.equals(state.getStatus()) || PolicyInstanceState.Status.RUNNING.equals(state.getStatus())) {
                return;
            }
            String writeAliasName = getWriteAliasName(settings.getStatic().stateLog().getAliasName());
            StateLogEntry stateLogEntry = new StateLogEntry(index, state);
            LOG.trace("Creating new state log entry\n{}", stateLogEntry.toPrettyJsonString());
            try {
                DocWriteResponse response = client.index(new IndexRequest(writeAliasName).source(stateLogEntry.toDocNode())).actionGet();
                if (!RestStatus.CREATED.equals(response.status())) {
                    LOG.debug("Failed to index state log entry. Response status was: {}", response.status().getStatus());
                }
            } catch (Exception e) {
                LOG.warn("Failed to index state log entry", e);
            }
        }
    }

    public static class StateLogInitializationException extends Exception {
        public StateLogInitializationException(String message) {
            super(message);
        }

        public StateLogInitializationException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public interface StateLogReadyListener {
        void onLogReady();

        void onLogFailure(Exception e);
    }

    public static class StateLogEntry implements Document<Object> {
        public static final String INDEX_FIELD = "index";
        public static final String TIMESTAMP_FIELD = "timestamp";
        public static final String STATE_FIELD = "state";

        private final String index;
        private final Instant timestamp;
        private final PolicyInstanceState state;

        protected StateLogEntry(String index, PolicyInstanceState state) {
            this.index = index;
            timestamp = Instant.now();
            this.state = state;
        }

        @Override
        public Object toBasicObject() {
            return ImmutableMap.of(INDEX_FIELD, index, TIMESTAMP_FIELD, timestamp.toString(), STATE_FIELD, state.toBasicObject());
        }
    }
}
