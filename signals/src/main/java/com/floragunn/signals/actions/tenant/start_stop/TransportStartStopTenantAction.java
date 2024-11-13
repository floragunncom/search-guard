package com.floragunn.signals.actions.tenant.start_stop;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.floragunn.signals.Signals;
import com.floragunn.signals.SignalsTenant;

public class TransportStartStopTenantAction extends HandledTransportAction<StartStopTenantRequest, StartStopTenantResponse> {
    private static final Logger log = LogManager.getLogger(TransportStartStopTenantAction.class);

    private final Signals signals;
    private final Client client;
    private final ThreadPool threadPool;

    @Inject
    public TransportStartStopTenantAction(Signals signals, TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
            Client client) {
        super(StartStopTenantAction.NAME, transportService, actionFilters, StartStopTenantRequest::new, threadPool.executor(ThreadPool.Names.GENERIC));

        this.signals = signals;
        this.client = client;
        this.threadPool = threadPool;
    }

    @Override
    protected final void doExecute(Task task, StartStopTenantRequest request, ActionListener<StartStopTenantResponse> listener) {
        try {

            ThreadContext threadContext = threadPool.getThreadContext();

            User user = threadContext.getTransient(ConfigConstants.SG_USER);

            if (user == null) {
                throw new RuntimeException("Request did not contain user");
            }

            SignalsTenant signalsTenant = signals.getTenant(user);

            if (signalsTenant == null) {
                throw new RuntimeException("No such tenant: " + user.getRequestedTenant());
            }

            this.threadPool.generic().submit(() -> {
                try {
                    signals.getSignalsSettings().update(client, "tenant." + signalsTenant.getName() + ".active",
                            String.valueOf(request.isActivate()));

                    listener.onResponse(new StartStopTenantResponse());
                } catch (Exception e) {
                    log.error("Error while start/stop tenant " + signalsTenant, e);
                    listener.onFailure(e);
                }
            });

        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}