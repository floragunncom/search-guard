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
package com.floragunn.signals.actions.watch.execute;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.floragunn.signals.SignalsTenant;
import com.google.common.base.Strings;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

class WatchRepository {

    private final SignalsTenant signalsTenant;

    private final ThreadPool threadPool;

    private final Client client;

    public WatchRepository(SignalsTenant signalsTenant, ThreadPool threadPool, Client client) {
        this.signalsTenant = Objects.requireNonNull(signalsTenant, "Signal tenant is required.");
        this.threadPool = Objects.requireNonNull(threadPool, "Thread pool is required");
        this.client = Objects.requireNonNull(client, "Client is required");
    }

    public CompletableFuture<GetResponse> findWatchById(User user, String watchId) {
        CompletableFuture<GetResponse> result = new CompletableFuture<>();
        ThreadContext threadContext = threadPool.getThreadContext();

        Object remoteAddress = threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS);
        Object origin = threadContext.getTransient(ConfigConstants.SG_ORIGIN);
        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            threadContext.putHeader(ConfigConstants.SG_CONF_REQUEST_HEADER, "true");
            threadContext.putTransient(ConfigConstants.SG_USER, user);
            threadContext.putTransient(ConfigConstants.SG_REMOTE_ADDRESS, remoteAddress);
            threadContext.putTransient(ConfigConstants.SG_ORIGIN, origin);

            client.prepareGet().setIndex(signalsTenant.getConfigIndexName()).setId(signalsTenant.getWatchIdForConfigIndex(watchId))
                .execute(new ListenerToFutureAdapter<>(result));
        }
        return result;
    }
    private static class ListenerToFutureAdapter<T> implements ActionListener<T> {

        private final CompletableFuture<T> future;

        public ListenerToFutureAdapter(CompletableFuture<T> future) {
            this.future = Objects.requireNonNull(future, "Future is required");
        }

        @Override
        public void onResponse(T response) {
            future.complete(response);
        }

        @Override
        public void onFailure(Exception e) {
            future.completeExceptionally(e);
        }
    }
}
