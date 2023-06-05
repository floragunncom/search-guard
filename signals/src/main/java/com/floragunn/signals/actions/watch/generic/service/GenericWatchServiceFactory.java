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
package com.floragunn.signals.actions.watch.generic.service;

import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.signals.Signals;
import com.floragunn.signals.actions.watch.generic.service.persistence.WatchInstancesRepository;
import com.floragunn.signals.actions.watch.generic.service.persistence.WatchStateRepository;
import org.elasticsearch.client.Client;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Objects;

public class GenericWatchServiceFactory {

    private final Signals signals;
    private final PrivilegedConfigClient client;

    public GenericWatchServiceFactory(Signals signals, Client client) {
        this.signals = Objects.requireNonNull(signals, "Signals singleton is required.");
        this.client = PrivilegedConfigClient.adapt(Objects.requireNonNull(client, "Client is required."));
    }

    public GenericWatchService create() {
        WatchInstancesRepository repository = new WatchInstancesRepository(client);
        WatchStateRepository stateRepository = new WatchStateRepository(client, signals);
        SchedulerConfigUpdateNotifier notifier = new SchedulerConfigUpdateNotifier(client, signals);
        return new GenericWatchService(signals, repository, stateRepository, notifier);
    }
}
