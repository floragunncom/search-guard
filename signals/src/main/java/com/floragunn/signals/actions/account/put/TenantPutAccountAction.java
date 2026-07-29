/*
 * Copyright 2026 floragunn GmbH
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

package com.floragunn.signals.actions.account.put;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.floragunn.signals.Signals;

public class TenantPutAccountAction extends ActionType<PutAccountResponse> {

    public static final TenantPutAccountAction INSTANCE = new TenantPutAccountAction();
    public static final String NAME = "cluster:admin:searchguard:tenant:signals:account/put";

    private TenantPutAccountAction() {
        super(NAME);
    }

    public static class TransportTenantPutAccountAction extends TransportPutAccountAction {

        @Inject
        public TransportTenantPutAccountAction(Signals signals, TransportService transportService, ThreadPool threadPool,
                ActionFilters actionFilters, Client client) {
            super(TenantPutAccountAction.NAME, signals, transportService, threadPool, actionFilters, client);
        }

        /**
         * Tenant put actions address accounts in the current user's tenant.
         */
        @Override
        protected boolean isTenantScoped() {
            return true;
        }
    }
}
