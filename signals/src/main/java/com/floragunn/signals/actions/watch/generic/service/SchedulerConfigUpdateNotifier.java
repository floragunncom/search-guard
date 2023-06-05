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
import com.floragunn.searchsupport.jobs.actions.SchedulerConfigUpdateAction;
import com.floragunn.signals.NoSuchTenantException;
import com.floragunn.signals.Signals;
import com.floragunn.signals.SignalsTenant;
import com.floragunn.signals.SignalsUnavailableException;

import java.util.Objects;

class SchedulerConfigUpdateNotifier {

    private final PrivilegedConfigClient privilegedConfigClient;

    private final Signals signals;

    public SchedulerConfigUpdateNotifier(PrivilegedConfigClient privilegedConfigClient, Signals signals) {
        this.privilegedConfigClient = Objects.requireNonNull(privilegedConfigClient, "Privileged client is required");
        this.signals = Objects.requireNonNull(signals, "Signals singleton is required");
    }

    public void send(String tenantName, Runnable afterSchedulerUpdateTask) {
        try {
            SignalsTenant signalsTenant = signals.getTenant(tenantName);
            SchedulerConfigUpdateAction.send(privilegedConfigClient, signalsTenant.getScopedName(), afterSchedulerUpdateTask);
        } catch (NoSuchTenantException | SignalsUnavailableException e) {
            String message = "Cannot send notification related to watch scheduler update after instance parameters modification";
            throw new RuntimeException(message, e);
        }
    }
}
