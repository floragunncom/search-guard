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
package com.floragunn.signals.actions.account.put;

import org.elasticsearch.action.ActionType;

public class PutAccountAction extends ActionType<PutAccountResponse> {

    public static final PutAccountAction INSTANCE = new PutAccountAction();
    public static final String NAME = "cluster:admin:searchguard:signals:account/put";

    protected PutAccountAction() {
        super(NAME, in -> {
            PutAccountResponse response = new PutAccountResponse(in);
            return response;
        });
    }
}
