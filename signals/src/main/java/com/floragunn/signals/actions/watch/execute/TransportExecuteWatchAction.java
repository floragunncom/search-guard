package com.floragunn.signals.actions.watch.execute;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.floragunn.signals.watch.common.throttle.ValidatingThrottlePeriodParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.UnexpectedDocumentStructureException;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.diag.DiagnosticContext;
import com.floragunn.signals.NoSuchTenantException;
import com.floragunn.signals.Signals;
import com.floragunn.signals.SignalsTenant;
import com.floragunn.signals.SignalsUnavailableException;
import com.floragunn.signals.actions.watch.execute.ExecuteWatchResponse.Status;
import com.floragunn.signals.execution.ExecutionEnvironment;
import com.floragunn.signals.execution.GotoCheckSelector;
import com.floragunn.signals.execution.WatchExecutionException;
import com.floragunn.signals.execution.WatchRunner;
import com.floragunn.signals.settings.SignalsSettings;
import com.floragunn.signals.support.NestedValueMap;
import com.floragunn.signals.support.ToXParams;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.init.WatchInitializationService;
import com.floragunn.signals.watch.result.WatchLog;
import com.floragunn.signals.watch.result.WatchLogIndexWriter;
import com.floragunn.signals.watch.result.WatchLogWriter;
import com.google.common.base.Charsets;

import static com.floragunn.signals.watch.common.ValidationLevel.LENIENT;

public class TransportExecuteWatchAction extends HandledTransportAction<ExecuteWatchRequest, ExecuteWatchResponse> {

    private static final Logger log = LogManager.getLogger(TransportExecuteWatchAction.class);

    private final Signals signals;
    private final Client client;
    private final ThreadPool threadPool;
    private final ScriptService scriptService;
    private final NamedXContentRegistry xContentRegistry;
    private final Settings settings;
    private final ClusterService clusterService;
    private final DiagnosticContext diagnosticContext;

    @Inject
    public TransportExecuteWatchAction(Signals signals, TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
            ScriptService scriptService, NamedXContentRegistry xContentRegistry, Client client, Settings settings, ClusterService clusterService,
            DiagnosticContext diagnosticContext) {
        super(ExecuteWatchAction.NAME, transportService, actionFilters, ExecuteWatchRequest::new, threadPool.executor(ThreadPool.Names.GENERIC));

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
    protected final void doExecute(Task task, ExecuteWatchRequest request, ActionListener<ExecuteWatchResponse> listener) {

        try {
            ThreadContext threadContext = threadPool.getThreadContext();

            User user = threadContext.getTransient(ConfigConstants.SG_USER);
            SignalsTenant signalsTenant = signals.getTenant(user);

            if (request.getWatchJson() != null) {
                executeAnonymousWatch(user, signalsTenant, task, request, listener);
            } else if (request.getWatchId() != null) {
                fetchAndExecuteWatch(user, signalsTenant, task, request, listener);
            }
        } catch (NoSuchTenantException e) {
            listener.onResponse(new ExecuteWatchResponse(e.getTenant(), request.getWatchId(), ExecuteWatchResponse.Status.TENANT_NOT_FOUND, null));
        } catch (SignalsUnavailableException e) {
            listener.onFailure(e.toElasticsearchException());
        } catch (Exception e) {
            listener.onFailure(e);
        } catch (Throwable t) {
            log.error(t);
        }
    }

    private void fetchAndExecuteWatch(User user, SignalsTenant signalsTenant, Task task, ExecuteWatchRequest request,
            ActionListener<ExecuteWatchResponse> listener) {
        ThreadContext threadContext = threadPool.getThreadContext();

        Object remoteAddress = threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS);
        Object origin = threadContext.getTransient(ConfigConstants.SG_ORIGIN);
        final Map<String, List<String>> originalResponseHeaders = threadContext.getResponseHeaders();

        try (StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            threadContext.putHeader(ConfigConstants.SG_CONF_REQUEST_HEADER, "true");
            threadContext.putTransient(ConfigConstants.SG_USER, user);
            threadContext.putTransient(ConfigConstants.SG_REMOTE_ADDRESS, remoteAddress);
            threadContext.putTransient(ConfigConstants.SG_ORIGIN, origin);

            originalResponseHeaders.entrySet().forEach(
                    h ->  h.getValue().forEach(v -> threadContext.addResponseHeader(h.getKey(), v))
            );

            client.prepareGet().setIndex(signalsTenant.getConfigIndexName()).setId(signalsTenant.getWatchIdForConfigIndex(request.getWatchId()))
                    .execute(new ActionListener<GetResponse>() {

                        @Override
                        public void onResponse(GetResponse response) {

                            try {
                                if (!response.isExists()) {
                                    listener.onResponse(new ExecuteWatchResponse(user != null ? user.getRequestedTenant() : null,
                                            request.getWatchId(), ExecuteWatchResponse.Status.NOT_FOUND, null));
                                    return;
                                }
                                WatchInitializationService initService = new WatchInitializationService(signals.getAccountRegistry(), scriptService,
                                        signals.getTruststoreRegistry(), signals.getHttpProxyHostRegistry(),
                                        new ValidatingThrottlePeriodParser(signals.getSignalsSettings()), signals.getSignalsScheduleFactory(), LENIENT
                                );
                                Watch watch = Watch.parse(initService, signalsTenant.getName(), request.getWatchId(),//
                                    response.getSourceAsString(), response.getVersion());

                                try (StoredContext ctx = threadPool.getThreadContext().stashContext()) {
                                    threadContext.putTransient(ConfigConstants.SG_USER, user);
                                    threadContext.putTransient(ConfigConstants.SG_REMOTE_ADDRESS, remoteAddress);
                                    threadContext.putTransient(ConfigConstants.SG_ORIGIN, origin);

                                    originalResponseHeaders.entrySet().forEach(
                                            h ->  h.getValue().forEach(v -> threadContext.addResponseHeader(h.getKey(), v))
                                    );

                                    listener.onResponse(executeWatch(watch, request, signalsTenant));

                                }

                            } catch (ConfigValidationException e) {
                                log.error("Invalid watch definition in fetchAndExecuteWatch(). This should not happen\n"
                                        + response.getSourceAsString() + "\n" + e.getValidationErrors(), e);
                                listener.onResponse(new ExecuteWatchResponse(signalsTenant.getName(), request.getWatchId(),
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

    private void executeAnonymousWatch(User user, SignalsTenant signalsTenant, Task task, ExecuteWatchRequest request,
            ActionListener<ExecuteWatchResponse> listener) {

        try {
            WatchInitializationService initService = new WatchInitializationService(signals.getAccountRegistry(), scriptService,
                    signals.getTruststoreRegistry(), signals.getHttpProxyHostRegistry(),
                    new ValidatingThrottlePeriodParser(signals.getSignalsSettings()), signals.getSignalsScheduleFactory(), LENIENT
            );
            Watch watch = Watch.parse(initService, signalsTenant.getName(),
                    "__inline_watch", request.getWatchJson(), -1);

            threadPool.generic().submit(threadPool.getThreadContext().preserveContext(() -> {
                try {
                    listener.onResponse(executeWatch(watch, request, signalsTenant));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }));

        } catch (ConfigValidationException e) {
            listener.onResponse(new ExecuteWatchResponse(signalsTenant.getName(), request.getWatchId(),
                    ExecuteWatchResponse.Status.INVALID_WATCH_DEFINITION, new BytesArray(e.toJsonString().getBytes(Charsets.UTF_8))));
        } catch (Exception e) {
            log.error("Error while executing anonymous watch " + request, e);
            listener.onFailure(e);
        }
    }

    private ExecuteWatchResponse executeWatch(Watch watch, ExecuteWatchRequest request, SignalsTenant signalsTenant) {

        WatchLogWriter watchLogWriter = null;
        NestedValueMap input = null;
        GotoCheckSelector checkSelector = null;

        ToXContent.Params watchLogToXparams = ToXParams.of(WatchLog.ToXContentParams.INCLUDE_DATA, !request.isIncludeAllRuntimeAttributesInResponse(),
                WatchLog.ToXContentParams.INCLUDE_RUNTIME_ATTRIBUTES, request.isIncludeAllRuntimeAttributesInResponse());

        if (request.isRecordExecution()) {
            watchLogWriter = WatchLogIndexWriter.forTenant(client, signalsTenant.getName(), new SignalsSettings(settings), watchLogToXparams);
        }

        if (request.getInputJson() != null) {
            try {
                input = NestedValueMap.fromJsonString(request.getInputJson());
            } catch (DocumentParseException | UnexpectedDocumentStructureException e) {
                log.info("Error while parsing json: " + request.getInputJson(), e);
                return new ExecuteWatchResponse(null, request.getWatchId(), Status.INVALID_INPUT, null);
            }
        }

        if (request.getGoTo() != null) {
            try {
                checkSelector = new GotoCheckSelector(watch, request.getGoTo());
            } catch (IllegalArgumentException e) {
                log.info("Error while parsing goTo: " + e);
                return new ExecuteWatchResponse(null, request.getWatchId(), Status.INVALID_GOTO, null);
            }
        }

        WatchRunner watchRunner = new WatchRunner(watch, client, signals.getAccountRegistry(), scriptService, watchLogWriter, null, diagnosticContext,
                null, ExecutionEnvironment.TEST, request.getSimulationMode(), xContentRegistry, signals.getSignalsSettings(),
                clusterService.getNodeName(), checkSelector, input, signals.getTruststoreRegistry(), signals.getClusterService(), signals.getFeatureService());

        try {
            WatchLog watchLog = watchRunner.execute();

            return new ExecuteWatchResponse(null, request.getWatchId(), Status.EXECUTED, toBytesReference(watchLog, watchLogToXparams));

        } catch (WatchExecutionException e) {
            log.info("Error while manually executing watch", e);
            return new ExecuteWatchResponse(null, request.getWatchId(), Status.ERROR_WHILE_EXECUTING,
                    toBytesReference(e.getWatchLog(), watchLogToXparams));
        }
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