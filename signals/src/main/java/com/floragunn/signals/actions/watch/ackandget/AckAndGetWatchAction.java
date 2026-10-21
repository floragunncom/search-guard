/*
 * Copyright 2023-2024 floragunn GmbH
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


package com.floragunn.signals.actions.watch.ackandget;

import org.elasticsearch.action.ActionType;

public class AckAndGetWatchAction extends ActionType<AckAndGetWatchResponse> {
    public static final AckAndGetWatchAction INSTANCE = new AckAndGetWatchAction();
    public static final String NAME = "cluster:admin:searchguard:tenant:signals:watch/ack_and_get";

    protected AckAndGetWatchAction() {
        super(NAME);
    }
}
