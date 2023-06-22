package com.floragunn.signals.actions.watch.template.service;

import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.signals.Signals;
import com.floragunn.signals.actions.watch.template.service.persistence.WatchParametersRepository;
import org.elasticsearch.client.Client;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Objects;

public class WatchTemplateServiceFactory {

    private final Signals signals;
    private final PrivilegedConfigClient client;
    private final ThreadPool threadPool;

    public WatchTemplateServiceFactory(Signals signals, Client client, ThreadPool threadPool) {
        this.signals = Objects.requireNonNull(signals, "Signals singleton is required.");
        this.client = PrivilegedConfigClient.adapt(Objects.requireNonNull(client, "Client is required"));
        this.threadPool = Objects.requireNonNull(threadPool, "Thread pool is required");
    }

    public WatchTemplateService create() {
        WatchParametersRepository repository = new WatchParametersRepository(client);
        SchedulerConfigUpdateNotifier notifier = new SchedulerConfigUpdateNotifier(client, threadPool, signals);
        return new WatchTemplateService(signals, repository, notifier);
    }
}
