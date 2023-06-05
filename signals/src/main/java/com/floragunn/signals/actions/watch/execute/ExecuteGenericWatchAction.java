/*
 * Copyright 2019-2023 floragunn GmbH
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
package com.floragunn.signals.actions.watch.execute;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.UnexpectedDocumentStructureException;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.OrderedImmutableMap;
import com.floragunn.searchguard.support.Base64Helper;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.searchsupport.diag.DiagnosticContext;
import com.floragunn.searchsupport.jobs.cluster.CurrentNodeJobSelector;
import com.floragunn.signals.NoSuchTenantException;
import com.floragunn.signals.Signals;
import com.floragunn.signals.SignalsTenant;
import com.floragunn.signals.SignalsUnavailableException;
import com.floragunn.signals.actions.watch.generic.service.WatchInstancesLoader;
import com.floragunn.signals.actions.watch.generic.service.persistence.WatchInstancesRepository;
import com.floragunn.signals.execution.ExecutionEnvironment;
import com.floragunn.signals.execution.GotoCheckSelector;
import com.floragunn.signals.execution.NotExecutableWatchException;
import com.floragunn.signals.execution.SimulationMode;
import com.floragunn.signals.execution.WatchExecutionException;
import com.floragunn.signals.execution.WatchRunner;
import com.floragunn.signals.settings.SignalsSettings;
import com.floragunn.signals.support.NestedValueMap;
import com.floragunn.signals.support.ToXParams;
import com.floragunn.signals.watch.GenericWatchInstanceFactory;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.common.throttle.ValidatingThrottlePeriodParser;
import com.floragunn.signals.watch.init.WatchInitializationService;
import com.floragunn.signals.watch.result.WatchLog;
import com.floragunn.signals.watch.result.WatchLogIndexWriter;
import com.floragunn.signals.watch.result.WatchLogWriter;
import com.google.common.base.Charsets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static com.floragunn.signals.watch.common.ValidationLevel.LENIENT;
import static com.floragunn.signals.watch.common.ValidationLevel.STRICT;

/**
 *   Deprecated action {@link ExecuteWatchAction} was not extensible, therefore it was impossible to add the new parameter
 *  {@link ExecuteGenericWatchRequest#instanceId} and preserve backwards compatibility. Therefore, the new action was implemented which
 *  accepts the additional parameter and should be more future-proof
 */
public class ExecuteGenericWatchAction extends
    Action<ExecuteGenericWatchAction.ExecuteGenericWatchRequest, ExecuteGenericWatchAction.ExecuteGenericWatchResponse> {

    private static final Logger log = LogManager.getLogger(ExecuteGenericWatchAction.class);

    public static final String NAME = "cluster:admin:searchguard:tenant:signals:watch/generic_execute";
    public static final ExecuteGenericWatchAction INSTANCE = new ExecuteGenericWatchAction();

    public ExecuteGenericWatchAction() {
        super(NAME, ExecuteGenericWatchRequest::new, ExecuteGenericWatchResponse::new);
    }

    public static class ExecuteGenericWatchHandler extends Handler<ExecuteGenericWatchRequest, ExecuteGenericWatchResponse> {

        private final Signals signals;
        private final Client client;
        private final ThreadPool threadPool;
        private final ScriptService scriptService;
        private final NamedXContentRegistry xContentRegistry;
        private final Settings settings;
        private final ClusterService clusterService;
        private final DiagnosticContext diagnosticContext;

        @Inject
        public ExecuteGenericWatchHandler(HandlerDependencies handlerDependencies, Signals signals, ThreadPool threadPool,
            ScriptService scriptService, NamedXContentRegistry xContentRegistry, Client client, Settings settings, ClusterService clusterService,
            DiagnosticContext diagnosticContext) {
            super(INSTANCE, handlerDependencies);
            this.signals = signals;
            this.client = client;
            this.threadPool = threadPool;
            this.scriptService = scriptService;
            this.xContentRegistry = xContentRegistry;
            this.settings = settings;
            this.clusterService = clusterService;
            this.diagnosticContext = diagnosticContext;
        }

        @Override
        protected CompletableFuture<ExecuteGenericWatchResponse> doExecute(ExecuteGenericWatchRequest request) {
            CompletableFuture<ExecuteGenericWatchResponse> responseFuture = new CompletableFuture<>();
            log.info("Thread before running execute action");
            executeAsync(request, new ActionListener<ExecuteGenericWatchResponse>() {
                @Override
                public void onResponse(ExecuteGenericWatchResponse executeGenericWatchResponse) {
                    responseFuture.complete(executeGenericWatchResponse);
                }

                @Override
                public void onFailure(Exception e) {
                    responseFuture.completeExceptionally(e);
                }
            });
            return responseFuture;
        }

        protected final void executeAsync(ExecuteGenericWatchRequest request, ActionListener<ExecuteGenericWatchResponse> listener) {
            try {
                ThreadContext threadContext = threadPool.getThreadContext();

                User user = threadContext.getTransient(ConfigConstants.SG_USER);
                SignalsTenant signalsTenant = signals.getTenant(user);

                if (request.getWatchJson() != null) {
                    log.debug("Execute anonymous watch.");
                    executeAnonymousWatch(signalsTenant, request, listener);
                } else if((request.getWatchId() != null) && request.getWatchInstanceId().isPresent()) {
                    log.debug("Execute generic watch instance or single instance watch '{}'.", request.getWatchId());
                    fetchAndExecuteWatchOrInstance(user, signalsTenant, request, listener);
                } else if (request.getWatchId() != null) {
                    // preferred method of execution single instance watch which is fully backwards compatible
                    log.debug("Execute single instance watch '{}'.", request.getWatchId());
                    fetchAndExecuteWatch(user, signalsTenant, request, listener);
                } else {
                    listener.onResponse(new ExecuteGenericWatchResponse(signalsTenant.getName(), request.getWatchId(), ExecuteWatchResponse.Status.MISSING_WATCH, null));
                    return;
                }
            } catch (NoSuchTenantException e) {
                listener.onResponse(new ExecuteGenericWatchResponse(e.getTenant(), request.getWatchId(), ExecuteWatchResponse.Status.TENANT_NOT_FOUND, null));
            } catch (SignalsUnavailableException e) {
                listener.onFailure(e.toElasticsearchException());
            } catch (Exception e) {
                listener.onFailure(e);
            } catch (Throwable t) {
                log.error(t);
            }
        }

        private void fetchAndExecuteWatch(User user, SignalsTenant signalsTenant, ExecuteGenericWatchRequest request,
            ActionListener<ExecuteGenericWatchResponse> listener) {
            ThreadContext threadContext = threadPool.getThreadContext();

            Object remoteAddress = threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS);
            Object origin = threadContext.getTransient(ConfigConstants.SG_ORIGIN);

            try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
                threadContext.putHeader(ConfigConstants.SG_CONF_REQUEST_HEADER, "true");
                threadContext.putTransient(ConfigConstants.SG_USER, user);
                threadContext.putTransient(ConfigConstants.SG_REMOTE_ADDRESS, remoteAddress);
                threadContext.putTransient(ConfigConstants.SG_ORIGIN, origin);

                client.prepareGet().setIndex(signalsTenant.getConfigIndexName()).setId(signalsTenant.getWatchIdForConfigIndex(request.getWatchId()))
                    .execute(new ActionListener<GetResponse>() {

                        @Override
                        public void onResponse(GetResponse response) {

                            try {
                                if (!response.isExists()) {
                                    listener.onResponse(new ExecuteGenericWatchResponse(user != null ? user.getRequestedTenant() : null,
                                        request.getWatchId(), ExecuteWatchResponse.Status.NOT_FOUND, null));
                                    return;
                                }
                                WatchInitializationService
                                    initService = new WatchInitializationService(signals.getAccountRegistry(), scriptService,
                                    signals.getTruststoreRegistry(), signals.getHttpProxyHostRegistry(), new ValidatingThrottlePeriodParser(signals.getSignalsSettings()), LENIENT);
                                Watch watch = Watch.parse(initService, signalsTenant.getName(), request.getWatchId(),//
                                    response.getSourceAsString(), response.getVersion());

                                try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
                                    threadContext.putTransient(ConfigConstants.SG_USER, user);
                                    threadContext.putTransient(ConfigConstants.SG_REMOTE_ADDRESS, remoteAddress);
                                    threadContext.putTransient(ConfigConstants.SG_ORIGIN, origin);
                                    threadPool.generic().submit(threadPool.getThreadContext().preserveContext(() -> {
                                        try {
                                            listener.onResponse(executeWatch(watch, null, request, signalsTenant));
                                        } catch (Exception e) {
                                            listener.onFailure(e);
                                        }
                                    }));

                                }

                            } catch (ConfigValidationException e) {
                                log.error("Invalid watch definition in fetchAndExecuteWatch(). This should not happen\n"
                                    + response.getSourceAsString() + "\n" + e.getValidationErrors(), e);
                                listener.onResponse(new ExecuteGenericWatchResponse(signalsTenant.getName(), request.getWatchId(),
                                    ExecuteWatchResponse.Status.INVALID_WATCH_DEFINITION,
                                    new BytesArray(e.toJsonString().getBytes(Charsets.UTF_8))));
                            } catch (Exception e) {
                                listener.onFailure(e);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(e);
                        }

                    });

            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        private void fetchAndExecuteWatchOrInstance(User user, SignalsTenant signalsTenant, ExecuteGenericWatchRequest request,
            ActionListener<ExecuteGenericWatchResponse> listener) {
            ThreadContext threadContext = threadPool.getThreadContext();
            Object remoteAddress = threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS);
            Object origin = threadContext.getTransient(ConfigConstants.SG_ORIGIN);

            WatchRepository watchRepository = new WatchRepository(signalsTenant, threadPool, client);
            watchRepository
                .findWatchById(user, request.getWatchId())//
                .thenAccept(response -> {
                    try {
                        if (!response.isExists()) {
                            listener.onResponse(new ExecuteGenericWatchResponse(user != null ? user.getRequestedTenant() : null,
                                request.getWatchId(), ExecuteWatchResponse.Status.NOT_FOUND, null));
                            return;
                        }
                        Watch watch = Watch.parse(new WatchInitializationService(signals.getAccountRegistry(), scriptService,
                                signals.getTruststoreRegistry(), signals.getHttpProxyHostRegistry(),
                                new ValidatingThrottlePeriodParser(signals.getSignalsSettings()), STRICT),
                            signalsTenant.getName(), request.getWatchId(), response.getSourceAsString(), response.getVersion());

                        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
                            threadContext.putTransient(ConfigConstants.SG_USER, user);
                            threadContext.putTransient(ConfigConstants.SG_REMOTE_ADDRESS, remoteAddress);
                            threadContext.putTransient(ConfigConstants.SG_ORIGIN, origin);
                            threadPool.generic().submit(threadPool.getThreadContext().preserveContext(() -> {
                                try {
                                    String watchInstanceId = request.getWatchInstanceId().orElse(null);
                                    listener.onResponse(executeWatch(watch, watchInstanceId, request, signalsTenant));
                                } catch (Exception e) {
                                    log.debug("Unexpected error during watch execution via REST API", e);
                                    listener.onFailure(e);
                                }
                            }));

                        }

                    } catch (ConfigValidationException e) {
                        log.error("Invalid watch definition in fetchAndExecuteWatch(). This should not happen\n"
                            + response.getSourceAsString() + "\n" + e.getValidationErrors(), e);
                        listener.onResponse(new ExecuteGenericWatchResponse(signalsTenant.getName(), request.getWatchId(),
                            ExecuteWatchResponse.Status.INVALID_WATCH_DEFINITION,
                            new BytesArray(e.toJsonString().getBytes(Charsets.UTF_8))));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                })
                .exceptionally(e -> {
                    log.debug("Watch execution via REST API finished with error", e);
                    if((e instanceof CompletionException) && (e.getCause() instanceof Exception)) {
                        listener.onFailure((Exception) e.getCause());
                    } if(e instanceof Exception) {
                        listener.onFailure((Exception) e);
                    } else {
                        listener.onFailure(new RuntimeException("Unexpected exception type", e));
                    }
                    return null;
                });
        }

        private void executeAnonymousWatch(SignalsTenant signalsTenant, ExecuteGenericWatchRequest request,
            ActionListener<ExecuteGenericWatchResponse> listener) {

            try {
                Watch watch = Watch.parse(new WatchInitializationService(signals.getAccountRegistry(), scriptService,
                        signals.getTruststoreRegistry(), signals.getHttpProxyHostRegistry(),
                        new ValidatingThrottlePeriodParser(signals.getSignalsSettings()), STRICT), signalsTenant.getName(),
                    "__inline_watch", request.getWatchJson(), -1);
                threadPool.generic().submit(threadPool.getThreadContext().preserveContext(() -> {
                    try {
                        listener.onResponse(executeWatch(watch, null, request, signalsTenant));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }));

            } catch (ConfigValidationException e) {
                listener.onResponse(new ExecuteGenericWatchResponse(signalsTenant.getName(), request.getWatchId(),
                    ExecuteWatchResponse.Status.INVALID_WATCH_DEFINITION, new BytesArray(e.toJsonString().getBytes(Charsets.UTF_8))));
            } catch (Exception e) {
                log.error("Error while executing anonymous watch " + request, e);
                listener.onFailure(e);
            }
        }

        private ExecuteGenericWatchResponse executeWatch(Watch watch, String instanceId, ExecuteGenericWatchRequest request, SignalsTenant signalsTenant) {
            if((!watch.isExecutable()) && (instanceId != null)) {
                GenericWatchInstanceFactory genericWatchInstanceFactory = createGenericWatchInstanceFactory(signalsTenant);
                Optional<Watch> genericWatchInstance = genericWatchInstanceFactory.instantiateOne(watch, instanceId);
                if(!genericWatchInstance.isPresent()) {
                    ToXContent errorMessage = errorMessage("Generic watch '" + watch.getId() +"' instance with id '" + instanceId + "' not found.");
                    return new ExecuteGenericWatchResponse(signalsTenant.getName(), request.getWatchId(), ExecuteWatchResponse.Status.INSTANCE_NOT_FOUND,
                        toBytesReference(errorMessage, ToXContent.EMPTY_PARAMS));
                }
                watch = genericWatchInstance.get();
            }

            WatchLogWriter watchLogWriter = null;
            NestedValueMap input = null;
            GotoCheckSelector checkSelector = null;

            ToXContent.Params watchLogToXparams = ToXParams.of(
                WatchLog.ToXContentParams.INCLUDE_DATA, !request.isIncludeAllRuntimeAttributesInResponse(),
                WatchLog.ToXContentParams.INCLUDE_RUNTIME_ATTRIBUTES, request.isIncludeAllRuntimeAttributesInResponse());

            if (request.isRecordExecution()) {
                watchLogWriter = WatchLogIndexWriter.forTenant(client, signalsTenant.getName(), new SignalsSettings(settings), watchLogToXparams);
            }

            if (request.getInputJson() != null) {
                try {
                    input = NestedValueMap.fromJsonString(request.getInputJson());
                } catch (DocumentParseException | UnexpectedDocumentStructureException e) {
                    log.info("Error while parsing json: " + request.getInputJson(), e);
                    return new ExecuteGenericWatchResponse(null, request.getWatchId(), ExecuteWatchResponse.Status.INVALID_INPUT, null);
                }
            }

            if (request.getGoTo() != null) {
                try {
                    checkSelector = new GotoCheckSelector(watch, request.getGoTo());
                } catch (IllegalArgumentException e) {
                    log.info("Error while parsing goTo: " + e);
                    return new ExecuteGenericWatchResponse(null, request.getWatchId(), ExecuteWatchResponse.Status.INVALID_GOTO, null);
                }
            }

            WatchRunner
                watchRunner = new WatchRunner(watch, client, signals.getAccountRegistry(), scriptService, watchLogWriter, null, diagnosticContext,
                null, ExecutionEnvironment.TEST, request.getSimulationMode(), xContentRegistry, signals.getSignalsSettings(),
                clusterService.getNodeName(), checkSelector, input, signals.getTruststoreRegistry());

            try {
                log.info("Thread which will be used to execute watch");
                WatchLog watchLog = watchRunner.execute();

                return new ExecuteGenericWatchResponse(null, request.getWatchId(), ExecuteWatchResponse.Status.EXECUTED, toBytesReference(watchLog, watchLogToXparams));

            } catch (WatchExecutionException e) {
                log.info("Error while manually executing watch", e);
                return new ExecuteGenericWatchResponse(null, request.getWatchId(), ExecuteWatchResponse.Status.ERROR_WHILE_EXECUTING,
                    toBytesReference(e.getWatchLog(), watchLogToXparams));
            } catch (NotExecutableWatchException e) {
                return new ExecuteGenericWatchResponse(null, request.getWatchId(), ExecuteWatchResponse.Status.NOT_EXECUTABLE_WATCH,
                    toBytesReference(errorMessage("Provided watch is not executable, is it generic watch definition?"), watchLogToXparams));
            }
        }

        private GenericWatchInstanceFactory createGenericWatchInstanceFactory(SignalsTenant signalsTenant) {
            ValidatingThrottlePeriodParser throttlePeriodParser = new ValidatingThrottlePeriodParser(signals.getSignalsSettings());
            WatchInitializationService initService = new WatchInitializationService(signals.getAccountRegistry(), scriptService,
                signals.getTruststoreRegistry(), signals.getHttpProxyHostRegistry(), throttlePeriodParser, STRICT);
            PrivilegedConfigClient privilegedConfigClient = PrivilegedConfigClient.adapt(client);
            WatchInstancesLoader instancesLoader = new WatchInstancesLoader(signalsTenant.getName(), new WatchInstancesRepository(privilegedConfigClient));
            return new GenericWatchInstanceFactory(instancesLoader, initService, CurrentNodeJobSelector.EXECUTE_ON_ALL_NODES);
        }

        private ToXContent errorMessage(String message) {
            return (xContentBuilder, params) -> {
                xContentBuilder.startObject();
                xContentBuilder.field("error", message);
                xContentBuilder.endObject();
                return xContentBuilder;
            };
        }

        private BytesReference toBytesReference(ToXContent toXContent, ToXContent.Params toXparams) {
            try {
                XContentBuilder builder = JsonXContent.contentBuilder();
                toXContent.toXContent(builder, toXparams);
                return BytesReference.bytes(builder);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class ExecuteGenericWatchRequest extends Action.Request {

        public static final String FIELD_WATCH_ID = "watch_id";
        public static final String FIELD_INSTANCE_ID = "instance_id";
        public static final String FIELD_WATCH_JSON = "watch_json";
        public static final String FIELD_RECORD_EXECUTION = "record_execution";
        public static final String FIELD_SIMULATION_MODE = "simulation_mode";
        public static final String FIELD_GOTO = "goto";
        public static final String FIELD_INPUT_JSON = "input_json";
        public static final String FIELD_INCLUDE_ALL_RUNTIME_ATTRIBUTES_IN_RESPONSE = "include_all_runtime_attributes_in_response";
        private final String watchId;
        private final String instanceId;
        private final String watchJson;
        private final boolean recordExecution;
        private final SimulationMode simulationMode;
        private String goTo;
        private String inputJson;
        private final boolean includeAllRuntimeAttributesInResponse;

        public ExecuteGenericWatchRequest(UnparsedMessage message) throws ConfigValidationException {
            DocNode docNode = message.requiredDocNode();
            this.watchId = docNode.getAsString(FIELD_WATCH_ID);
            this.instanceId = docNode.getAsString(FIELD_INSTANCE_ID);
            this.watchJson = docNode.getAsString(FIELD_WATCH_JSON);
            this.recordExecution = docNode.getBoolean(FIELD_RECORD_EXECUTION);
            String simultionModeString = docNode.getAsString(FIELD_SIMULATION_MODE);
            this.simulationMode = simultionModeString == null ? null : SimulationMode.valueOf(simultionModeString);
            this.goTo = docNode.getAsString(FIELD_GOTO);
            this.inputJson = docNode.getAsString(FIELD_INPUT_JSON);
            this.includeAllRuntimeAttributesInResponse = docNode.getBoolean(FIELD_INCLUDE_ALL_RUNTIME_ATTRIBUTES_IN_RESPONSE);
        }

        public ExecuteGenericWatchRequest(String watchId, String instanceId, String watchJson, boolean recordExecution, SimulationMode simulationMode,
            boolean includeAllRuntimeAttributesInResponse) {
            super();
            this.watchId = watchId;
            this.instanceId = instanceId;
            this.watchJson = watchJson;
            this.recordExecution = recordExecution;
            this.simulationMode = simulationMode;
            this.includeAllRuntimeAttributesInResponse = includeAllRuntimeAttributesInResponse;
        }

        @Override
        public Object toBasicObject() {
            String simulationModeString = simulationMode == null ? null : simulationMode.name();
            return ImmutableMap.of(FIELD_WATCH_ID, watchId, FIELD_INSTANCE_ID, instanceId, FIELD_WATCH_JSON, watchJson,
                FIELD_RECORD_EXECUTION, recordExecution, FIELD_SIMULATION_MODE, simulationModeString)
                .with(ImmutableMap.of(FIELD_GOTO, goTo, FIELD_INPUT_JSON, inputJson,
                    FIELD_INCLUDE_ALL_RUNTIME_ATTRIBUTES_IN_RESPONSE, includeAllRuntimeAttributesInResponse));
        }

        public String getWatchId() {
            return watchId;
        }

        public String getWatchJson() {
            return watchJson;
        }

        public boolean isRecordExecution() {
            return recordExecution;
        }

        public SimulationMode getSimulationMode() {
            return simulationMode;
        }

        public String getGoTo() {
            return goTo;
        }

        public String getInputJson() {
            return inputJson;
        }

        public boolean isIncludeAllRuntimeAttributesInResponse() {
            return includeAllRuntimeAttributesInResponse;
        }

        public Optional<String> getWatchInstanceId() {
            return Optional.ofNullable(instanceId);
        }

        public void setGoTo(String goTo) {
            this.goTo = goTo;
        }

        public void setInputJson(String inputJson) {
            this.inputJson = inputJson;
        }
    }

    public static class ExecuteGenericWatchResponse extends Action.Response {
        public static final String FIELD_TENANT = "tenant";
        public static final String FIELD_ID = "id";
        public static final String FIELD_STATUS = "status";
        public static final String FIELD_RESULT = "result";
        private final String tenant;
        private final String id;
        private final ExecuteWatchResponse.Status status;
        private final BytesReference result;

        public ExecuteGenericWatchResponse(UnparsedMessage message) throws ConfigValidationException {
            super(message);
            DocNode docNode = message.requiredDocNode();
            this.tenant = docNode.getAsString(FIELD_TENANT);
            this.id = docNode.getAsString(FIELD_ID);
            String stringStatus = docNode.getAsString(FIELD_STATUS);
            this.status = stringStatus == null ? null : ExecuteWatchResponse.Status.valueOf(stringStatus);
            this.result = Optional.ofNullable(docNode.getAsString(FIELD_RESULT)) //
                .map(Base64Helper::deserializeObject) //
                .map(byte[].class::cast) //
                .map(BytesArray::new) //
                .orElse(null);
        }

        public ExecuteGenericWatchResponse(String tenant, String id, ExecuteWatchResponse.Status status, BytesReference result) {
            this.tenant = tenant;
            this.id = id;
            this.status = status;
            this.result = result;
        }

        @Override
        public OrderedImmutableMap<String, Object> toBasicObject() {
            String serializedResult = Optional.ofNullable(result)//
                .map(BytesReference::array) //
                .map(Base64Helper::serializeObject) //
                .orElse(null);
            String statusName = status == null ? null : status.name();
            return OrderedImmutableMap.of(FIELD_TENANT, tenant, FIELD_ID, id, FIELD_STATUS, statusName, FIELD_RESULT, serializedResult);
        }

        public ExecuteWatchResponse.Status getExecutionStatus() {
            return status;
        }

        public BytesReference getResult() {
            return result;
        }

        public String getTenant() {
            return tenant;
        }

        public String getId() {
            return id;
        }
    }
}
