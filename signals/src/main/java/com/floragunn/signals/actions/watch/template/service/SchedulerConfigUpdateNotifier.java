package com.floragunn.signals.actions.watch.template.service;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.jobs.actions.SchedulerConfigUpdateAction;
import com.floragunn.signals.NoSuchTenantException;
import com.floragunn.signals.Signals;
import com.floragunn.signals.SignalsTenant;
import com.floragunn.signals.SignalsUnavailableException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Objects;

class SchedulerConfigUpdateNotifier {

    private final PrivilegedConfigClient privilegedConfigClient;

    private final ThreadPool threadPool;

    private final Signals signals;

    public SchedulerConfigUpdateNotifier(PrivilegedConfigClient privilegedConfigClient, ThreadPool threadPool, Signals signals) {
        this.privilegedConfigClient = Objects.requireNonNull(privilegedConfigClient, "Privileged client is required");
        this.threadPool = Objects.requireNonNull(threadPool, "Thread pool is required");
        this.signals = Objects.requireNonNull(signals, "Signals singleton is required");
    }

    public void send() {
        ThreadContext threadContext = threadPool.getThreadContext();
        // TODO can user be null when we have PrivilegedConfigClient ?
        User user = threadContext.getTransient(ConfigConstants.SG_USER);
        try {
            SignalsTenant signalsTenant = signals.getTenant(user);
            SchedulerConfigUpdateAction.send(privilegedConfigClient, signalsTenant.getScopedName());
        } catch (NoSuchTenantException | SignalsUnavailableException e) {
            String message = "Cannot send notification related to watch scheduler update after instance parameters modification";
            throw new RuntimeException(message, e);
        }
    }
}
