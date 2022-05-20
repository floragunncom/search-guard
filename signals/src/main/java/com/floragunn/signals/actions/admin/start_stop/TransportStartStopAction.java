package com.floragunn.signals.actions.admin.start_stop;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

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
        super(StartStopAction.NAME, transportService, actionFilters, StartStopRequest::new);

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