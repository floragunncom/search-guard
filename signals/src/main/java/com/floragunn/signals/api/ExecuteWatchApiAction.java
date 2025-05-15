package com.floragunn.signals.api;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.floragunn.signals.Signals;
import com.floragunn.signals.proxy.service.HttpProxyHostRegistry;
import com.floragunn.signals.truststore.service.TrustManagerRegistry;
import com.floragunn.signals.watch.common.throttle.ThrottlePeriodParser;
import com.floragunn.signals.watch.common.throttle.ValidatingThrottlePeriodParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptService;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocWriter;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.signals.actions.watch.execute.ExecuteWatchAction;
import com.floragunn.signals.actions.watch.execute.ExecuteWatchRequest;
import com.floragunn.signals.actions.watch.execute.ExecuteWatchResponse;
import com.floragunn.signals.execution.SimulationMode;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.init.WatchInitializationService;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import static com.floragunn.signals.watch.common.ValidationLevel.LENIENT;

public class ExecuteWatchApiAction extends SignalsTenantAwareRestHandler {

    private final Logger log = LogManager.getLogger(this.getClass());
    private final ScriptService scriptService;
    private final ThrottlePeriodParser throttlePeriodParser;
    private final TrustManagerRegistry trustManagerRegistry;
    private final HttpProxyHostRegistry httpProxyHostRegistry;

    public ExecuteWatchApiAction(Settings settings, ScriptService scriptService, Signals signals) {
        super(settings);
        this.scriptService = scriptService;
        this.throttlePeriodParser = new ValidatingThrottlePeriodParser(signals.getSignalsSettings());
        this.trustManagerRegistry = signals.getTruststoreRegistry();
        this.httpProxyHostRegistry = signals.getHttpProxyHostRegistry();
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(Method.POST, "/_signals/watch/{tenant}/_execute"),
                new Route(Method.POST, "/_signals/watch/{tenant}/{id}/_execute"));
    }

    @Override
    protected final RestChannelConsumer getRestChannelConsumer(RestRequest request, NodeClient client) throws IOException {
        try {

            final String id = request.param("id");

            //we need to consume the tenant param here because
            //if not ES 8 throws an exception
            request.param("tenant");
            WatchInitializationService watchInitializationService = new WatchInitializationService(null, scriptService,
                trustManagerRegistry, httpProxyHostRegistry, throttlePeriodParser, LENIENT);
            final RequestBody requestBody = RequestBody.parse(watchInitializationService, request.content().utf8ToString());

            if (log.isDebugEnabled()) {
                log.debug("Execute watch " + id + ":\n" + requestBody);
            }

            SimulationMode simulationMode = requestBody.isSkipActions() ? SimulationMode.SKIP_ACTIONS
                    : requestBody.isSimulate() ? SimulationMode.SIMULATE_ACTIONS : SimulationMode.FOR_REAL;

            ExecuteWatchRequest executeWatchRequest = new ExecuteWatchRequest(id,
                    requestBody.getWatch() != null ? requestBody.getWatch().toJson() : null, requestBody.isRecordExecution(), simulationMode,
                    requestBody.isShowAllRuntimeAttributes());

            if (requestBody.getInput() != null) {
                executeWatchRequest.setInputJson(DocWriter.json().writeAsString(requestBody.getInput()));
            }

            executeWatchRequest.setGoTo(requestBody.getGoTo());

            return channel -> {
                client.execute(ExecuteWatchAction.INSTANCE, executeWatchRequest, new ActionListener<ExecuteWatchResponse>() {

                    @Override
                    public void onResponse(ExecuteWatchResponse response) {
                        if (response.getStatus() == ExecuteWatchResponse.Status.EXECUTED) {
                            channel.sendResponse(new RestResponse(RestStatus.OK, "application/json", response.getResult()));
                        } else if (response.getStatus() == ExecuteWatchResponse.Status.NOT_FOUND) {
                            errorResponse(channel, RestStatus.NOT_FOUND, "No watch with id " + id);
                        } else if (response.getStatus() == ExecuteWatchResponse.Status.TENANT_NOT_FOUND) {
                            errorResponse(channel, RestStatus.NOT_FOUND, "Tenant does not exist");
                        } else if (response.getStatus() == ExecuteWatchResponse.Status.ERROR_WHILE_EXECUTING) {
                            channel.sendResponse(new RestResponse(RestStatus.UNPROCESSABLE_ENTITY, "application/json", response.getResult()));
                        } else if (response.getStatus() == ExecuteWatchResponse.Status.INVALID_WATCH_DEFINITION) {
                            if (requestBody.getWatch() != null) {
                                channel.sendResponse(new RestResponse(RestStatus.BAD_REQUEST, "application/json", response.getResult()));
                            } else {
                                errorResponse(channel, RestStatus.INTERNAL_SERVER_ERROR, "Internal Server Error: Stored watch cannot be parsed");
                            }
                        } else if (response.getStatus() == ExecuteWatchResponse.Status.INVALID_GOTO) {
                            errorResponse(channel, RestStatus.BAD_REQUEST, "Invalid goto value: " + requestBody.getGoTo());
                        } else {
                            errorResponse(channel, RestStatus.INTERNAL_SERVER_ERROR, "Internal Server Error");
                        }

                    }

                    @Override
                    public void onFailure(Exception e) {
                        log.error("Error while executing watch", e);
                        errorResponse(channel, e);
                    }
                });
            };

        } catch (ConfigValidationException e) {
            log.info("Error while parsing request body", e);

            return channel -> {
                errorResponse(channel, RestStatus.BAD_REQUEST, e.getMessage(), e.getValidationErrors().toJsonString());
            };
        }
    }

    @Override
    public String getName() {
        return "Execute Watch";
    }

    static class RequestBody {
        private Map<String, Object> input;
        private boolean recordExecution;
        private boolean simulate;
        private boolean skipActions;
        private boolean showAllRuntimeAttributes;
        private String goTo;
        private Watch watch;

        public Map<String, Object> getInput() {
            return input;
        }

        public void setInput(Map<String, Object> input) {
            this.input = input;
        }

        public boolean isRecordExecution() {
            return recordExecution;
        }

        public void setRecordExecution(boolean recordExecution) {
            this.recordExecution = recordExecution;
        }

        public Watch getWatch() {
            return watch;
        }

        public void setWatch(Watch watch) {
            this.watch = watch;
        }

        static RequestBody parse(WatchInitializationService initContext, String requestBody) throws ConfigValidationException {
            if (Strings.isNullOrEmpty(requestBody)) {
                return new RequestBody();
            }

            DocNode rootNode = DocNode.parse(Format.JSON).from(requestBody);
            RequestBody result = new RequestBody();

            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vNode = new ValidatingDocNode(rootNode, validationErrors);

            if (rootNode.hasNonNull("watch")) {
                try {
                    result.watch = Watch.parse(initContext, "anon", "anon_" + UUID.randomUUID(), rootNode.getAsNode("watch"));
                } catch (ConfigValidationException e) {
                    validationErrors.add("watch", e);
                }
            }

            if (rootNode.hasNonNull("input")) {
                result.input = rootNode.getAsNode("input").toMap();
            }

            if (rootNode.hasNonNull("record_execution")) {
                result.recordExecution = vNode.get("record_execution").asBoolean();
            }

            if (rootNode.hasNonNull("simulate")) {
                result.simulate = vNode.get("simulate").asBoolean();
            }

            if (rootNode.hasNonNull("skip_actions")) {
                result.skipActions = vNode.get("skip_actions").asBoolean();
            }

            if (rootNode.hasNonNull("goto")) {
                result.goTo = rootNode.getAsString("goto");
            }

            if (rootNode.hasNonNull("show_all_runtime_attributes")) {
                result.showAllRuntimeAttributes = vNode.get("show_all_runtime_attributes").asBoolean();
            }

            validationErrors.throwExceptionForPresentErrors();

            return result;

        }

        @Override
        public String toString() {
            return "RequestBody [alternativeInput=" + input + ", recordExecution=" + recordExecution + ", watch=" + watch + "]";
        }

        public boolean isSimulate() {
            return simulate;
        }

        public void setSimulate(boolean simulate) {
            this.simulate = simulate;
        }

        public boolean isSkipActions() {
            return skipActions;
        }

        public void setSkipActions(boolean skipActions) {
            this.skipActions = skipActions;
        }

        public String getGoTo() {
            return goTo;
        }

        public void setGoTo(String goTo) {
            this.goTo = goTo;
        }

        public boolean isShowAllRuntimeAttributes() {
            return showAllRuntimeAttributes;
        }

        public void setShowAllRuntimeAttributes(boolean showAllRuntimeAttributes) {
            this.showAllRuntimeAttributes = showAllRuntimeAttributes;
        }
    }
}
