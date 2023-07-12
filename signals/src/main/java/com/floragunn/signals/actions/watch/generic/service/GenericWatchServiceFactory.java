package com.floragunn.signals.actions.watch.generic.service;

import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.signals.Signals;
import com.floragunn.signals.actions.watch.generic.service.persistence.WatchInstancesRepository;
import org.elasticsearch.client.Client;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Objects;

public class GenericWatchServiceFactory {

    private final Signals signals;
    private final PrivilegedConfigClient client;
    private final ThreadPool threadPool;

    public GenericWatchServiceFactory(Signals signals, Client client, ThreadPool threadPool) {
        this.signals = Objects.requireNonNull(signals, "Signals singleton is required.");
        this.client = PrivilegedConfigClient.adapt(Objects.requireNonNull(client, "Client is required"));
        this.threadPool = Objects.requireNonNull(threadPool, "Thread pool is required");
    }

    public GenericWatchService create() {
        WatchInstancesRepository repository = new WatchInstancesRepository(client);
        SchedulerConfigUpdateNotifier notifier = new SchedulerConfigUpdateNotifier(client, threadPool, signals);
        return new GenericWatchService(signals, repository, notifier);
    }
}
