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
package com.floragunn.signals.actions.watch.generic.rest;

import com.floragunn.searchsupport.action.Action;
import com.floragunn.searchsupport.action.Action.Handler;
import com.floragunn.searchsupport.action.Action.HandlerDependencies;
import com.floragunn.searchsupport.action.Action.Request;
import com.floragunn.searchsupport.action.StandardResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.CompletableFuture;

abstract class StandardSyncOperationHandler<RequestType extends Request> extends Handler<RequestType, StandardResponse> {

    private static final Logger log = LogManager.getLogger(StandardSyncOperationHandler.class);

    public StandardSyncOperationHandler(Action<RequestType, StandardResponse> action, HandlerDependencies dependencies, ThreadPool threadPool) {
        super(action, dependencies);
    }

    @Override
    final protected CompletableFuture<StandardResponse> doExecute(RequestType requestType) {
        return supplyAsync(() -> {
            try {
                return synchronousExecute(requestType);
            } catch (Exception ex) {
                log.error("Unexpected error occurred during action '{}' execution.", this.actionName, ex);
                return new StandardResponse(ex);
            }
        });
    }

    abstract protected StandardResponse synchronousExecute(RequestType requestType);

}
