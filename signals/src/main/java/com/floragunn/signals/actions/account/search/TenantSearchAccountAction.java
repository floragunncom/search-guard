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

package com.floragunn.signals.actions.account.search;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.floragunn.signals.Signals;

public class TenantSearchAccountAction extends ActionType<SearchAccountResponse> {

    public static final TenantSearchAccountAction INSTANCE = new TenantSearchAccountAction();
    public static final String NAME = "cluster:admin:searchguard:tenant:signals:account/search";

    private TenantSearchAccountAction() {
        super(NAME);
    }

    public static class TransportTenantSearchAccountAction extends TransportSearchAccountAction {

        @Inject
        public TransportTenantSearchAccountAction(Signals signals, TransportService transportService, ThreadPool threadPool,
                ActionFilters actionFilters, Client client) {
            super(TenantSearchAccountAction.NAME, signals, transportService, threadPool, actionFilters, client);
        }

        /**
         * Tenant search actions include accounts in the current user's tenant.
         */
        @Override
        protected boolean isTenantScoped() {
            return true;
        }
    }
}
