package com.floragunn.signals.actions.admin.start_stop;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.floragunn.signals.Signals;
import com.floragunn.signals.settings.SignalsSettings;

public class TransportStartStopAction extends HandledTransportAction<StartStopRequest, StartStopResponse> {
    private static final Logger log = LogManager.getLogger(TransportStartStopAction.class);

    private final Signals signals;
    private final Client client;
    private final ThreadPool threadPool;

    @Inject
    public TransportStartStopAction(Signals signals, TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
            Client client) {
        super(StartStopAction.NAME, transportService, actionFilters, StartStopRequest::new, threadPool.executor(ThreadPool.Names.GENERIC));

        this.signals = signals;
        this.client = client;
        this.threadPool = threadPool;
    }

    @Override
    protected final void doExecute(Task task, StartStopRequest request, ActionListener<StartStopResponse> listener) {
        try {

            this.threadPool.generic().submit(() -> {
                try {
                    signals.getSignalsSettings().update(client, SignalsSettings.DynamicSettings.ACTIVE.getKey(),
                            String.valueOf(request.isActivate()));

                    listener.onResponse(new StartStopResponse());
                } catch (Exception e) {
                    log.error("Error while start/stop", e);
                    listener.onFailure(e);
                }
            });

        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}