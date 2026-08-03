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

package com.floragunn.signals.actions.account.delete;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.floragunn.signals.Signals;

public class TenantDeleteAccountAction extends ActionType<DeleteAccountResponse> {

    public static final TenantDeleteAccountAction INSTANCE = new TenantDeleteAccountAction();
    public static final String NAME = "cluster:admin:searchguard:tenant:signals:account/delete";

    private TenantDeleteAccountAction() {
        super(NAME);
    }

    public static class TransportTenantDeleteAccountAction extends TransportDeleteAccountAction {

        @Inject
        public TransportTenantDeleteAccountAction(Signals signals, TransportService transportService, ThreadPool threadPool,
                ActionFilters actionFilters, Client client) {
            super(TenantDeleteAccountAction.NAME, signals, transportService, threadPool, actionFilters, client);
        }

        /**
         * Tenant delete actions address accounts in the current user's tenant.
         */
        @Override
        protected boolean isTenantScoped() {
            return true;
        }
    }
}
