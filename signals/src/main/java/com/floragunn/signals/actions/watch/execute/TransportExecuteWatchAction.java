package com.floragunn.signals.actions.watch.execute;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.jobs.config.validation.ConfigValidationException;
import com.floragunn.signals.Signals;
import com.floragunn.signals.SignalsSettings;
import com.floragunn.signals.SignalsTenant;
import com.floragunn.signals.actions.watch.execute.ExecuteWatchResponse.Status;
import com.floragunn.signals.execution.ExecutionEnvironment;
import com.floragunn.signals.execution.GotoCheckSelector;
import com.floragunn.signals.execution.WatchExecutionException;
import com.floragunn.signals.execution.WatchRunner;
import com.floragunn.signals.support.NestedValueMap;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.init.WatchInitializationService;
import com.floragunn.signals.watch.result.WatchLog;
import com.floragunn.signals.watch.result.WatchLogIndexWriter;
import com.floragunn.signals.watch.result.WatchLogWriter;

public class TransportExecuteWatchAction extends HandledTransportAction<ExecuteWatchRequest, ExecuteWatchResponse> {

    private static final Logger log = LogManager.getLogger(TransportExecuteWatchAction.class);

    private final Signals signals;
    private final Client client;
    private final ThreadPool threadPool;
    private final ScriptService scriptService;
    private final NamedXContentRegistry xContentRegistry;
    private final Settings settings;
    private final ClusterService clusterService;

    @Inject
    public TransportExecuteWatchAction(Signals signals, TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
            ScriptService scriptService, NamedXContentRegistry xContentRegistry, Client client, Settings settings, ClusterService clusterService) {
        super(ExecuteWatchAction.NAME, transportService, actionFilters, ExecuteWatchRequest::new);

        this.signals = signals;
        this.client = client;
        this.threadPool = threadPool;
        this.scriptService = scriptService;
        this.xContentRegistry = xContentRegistry;
        this.settings = settings;
        this.clusterService = clusterService;
    }

    @Override
    protected final void doExecute(Task task, ExecuteWatchRequest request, ActionListener<ExecuteWatchResponse> listener) {

        ThreadContext threadContext = threadPool.getThreadContext();

        User user = threadContext.getTransient(ConfigConstants.SG_USER);
        SignalsTenant signalsTenant = signals.getTenant(user);

        if (signalsTenant == null) {
            listener.onResponse(new ExecuteWatchResponse(user != null ? user.getRequestedTenant() : null, request.getWatchId(),
                    ExecuteWatchResponse.Status.TENANT_NOT_FOUND, null));
            return;
        }

        if (request.getWatchJson() != null) {
            executeAnonymousWatch(user, signalsTenant, task, request, listener);
        } else if (request.getWatchId() != null) {
            fetchAndExecuteWatch(user, signalsTenant, task, request, listener);
        }
    }

    private void fetchAndExecuteWatch(User user, SignalsTenant signalsTenant, Task task, ExecuteWatchRequest request,
            ActionListener<ExecuteWatchResponse> listener) {
        ThreadContext threadContext = threadPool.getThreadContext();

        Object remoteAddress = threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS);
        Object origin = threadContext.getTransient(ConfigConstants.SG_ORIGIN);

        try (StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            threadContext.putHeader(ConfigConstants.SG_CONF_REQUEST_HEADER, "true");
            threadContext.putTransient(ConfigConstants.SG_USER, user);
            threadContext.putTransient(ConfigConstants.SG_REMOTE_ADDRESS, remoteAddress);
            threadContext.putTransient(ConfigConstants.SG_ORIGIN, origin);

            client.prepareGet(signalsTenant.getConfigIndexName(), null, signalsTenant.getWatchIdForConfigIndex(request.getWatchId()))
                    .execute(new ActionListener<GetResponse>() {

                        @Override
                        public void onResponse(GetResponse response) {

                            try {
                                if (!response.isExists()) {
                                    listener.onResponse(new ExecuteWatchResponse(user != null ? user.getRequestedTenant() : null,
                                            request.getWatchId(), ExecuteWatchResponse.Status.NOT_FOUND, null));
                                    return;
                                }

                                Watch watch = Watch.parse(new WatchInitializationService(signals.getAccountRegistry(), scriptService),
                                        signalsTenant.getName(), request.getWatchId(), response.getSourceAsString(), response.getVersion());

                                try (StoredContext ctx = threadPool.getThreadContext().stashContext()) {
                                    threadContext.putTransient(ConfigConstants.SG_USER, user);
                                    threadContext.putTransient(ConfigConstants.SG_REMOTE_ADDRESS, remoteAddress);
                                    threadContext.putTransient(ConfigConstants.SG_ORIGIN, origin);

                                    listener.onResponse(executeWatch(watch, request, signalsTenant));

                                }

                            } catch (ConfigValidationException e) {
                                log.error("Invalid watch definition in fetchAndExecuteWatch(). This should not happen\n"
                                        + response.getSourceAsString() + "\n" + e.getValidationErrors(), e);
                                listener.onResponse(new ExecuteWatchResponse(signalsTenant.getName(), request.getWatchId(),
                                        ExecuteWatchResponse.Status.INVALID_WATCH_DEFINITION, toBytesReference(e)));
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
            Watch watch = Watch.parse(new WatchInitializationService(signals.getAccountRegistry(), scriptService), signalsTenant.getName(),
                    "inline_watch", request.getWatchJson(), -1);

            threadPool.generic().submit(() -> {
                listener.onResponse(executeWatch(watch, request, signalsTenant));
            });

        } catch (ConfigValidationException e) {
            listener.onResponse(new ExecuteWatchResponse(signalsTenant.getName(), request.getWatchId(),
                    ExecuteWatchResponse.Status.INVALID_WATCH_DEFINITION, toBytesReference(e)));
        } catch (Exception e) {
            log.error("Error while executing anonymous watch " + request, e);
            listener.onFailure(e);
        }
    }

    private ExecuteWatchResponse executeWatch(Watch watch, ExecuteWatchRequest request, SignalsTenant signalsTenant) {

        WatchLogWriter watchLogWriter = null;
        NestedValueMap input = null;
        GotoCheckSelector checkSelector = null;

        if (request.isRecordExecution()) {
            watchLogWriter = WatchLogIndexWriter.forTenant(client, signalsTenant.getName(), new SignalsSettings(settings));
        }

        if (request.getInputJson() != null) {
            try {
                input = NestedValueMap.fromJsonString(request.getInputJson());
            } catch (IOException e) {
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

        WatchRunner watchRunner = new WatchRunner(watch, client, signals.getAccountRegistry(), scriptService, watchLogWriter, null, null,
                ExecutionEnvironment.TEST, request.getSimulationMode(), xContentRegistry, signals.getSignalsSettings(), clusterService.getNodeName(),
                checkSelector, input);

        try {
            WatchLog watchLog = watchRunner.execute();

            return new ExecuteWatchResponse(null, request.getWatchId(), Status.EXECUTED, toBytesReference(watchLog));

        } catch (WatchExecutionException e) {
            log.info("Error while manually executing watch", e);
            return new ExecuteWatchResponse(null, request.getWatchId(), Status.ERROR_WHILE_EXECUTING, toBytesReference(e.getWatchLog()));
        }
    }

    private BytesReference toBytesReference(ToXContent toXContent) {
        try {
            XContentBuilder builder = JsonXContent.contentBuilder();
            toXContent.toXContent(builder, ToXContent.EMPTY_PARAMS);
            return BytesReference.bytes(builder);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}