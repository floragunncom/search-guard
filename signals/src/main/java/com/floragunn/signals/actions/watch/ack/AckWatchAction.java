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
package com.floragunn.signals.actions.watch.ack;

import org.elasticsearch.action.ActionType;

public class AckWatchAction extends ActionType<AckWatchResponse> {
    public static final AckWatchAction INSTANCE = new AckWatchAction();
    public static final String NAME = "cluster:admin:searchguard:tenant:signals:watch/ack";

    protected AckWatchAction() {
        super(NAME, in -> {
            AckWatchResponse response = new AckWatchResponse(in);
            return response;
        });
    }
}
