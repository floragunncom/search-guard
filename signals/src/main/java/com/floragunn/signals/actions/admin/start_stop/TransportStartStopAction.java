/*
 * Copyright 2023 floragunn GmbH
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
package com.floragunn.signals.actions.admin.start_stop;

import com.floragunn.signals.Signals;
import com.floragunn.signals.settings.SignalsSettings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

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
