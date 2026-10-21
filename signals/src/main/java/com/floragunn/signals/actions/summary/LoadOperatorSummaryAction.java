/*
 * Copyright 2023-2025 floragunn GmbH
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

package com.floragunn.signals.actions.summary;

import com.floragunn.searchsupport.action.Action;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardResponse;

public class LoadOperatorSummaryAction extends Action<LoadOperatorSummaryRequest, StandardResponse> {

    public static final String NAME = "cluster:admin:searchguard:signals:summary/load";
    public static final LoadOperatorSummaryAction INSTANCE = new LoadOperatorSummaryAction();

    public static final RestApi REST_API = new RestApi()//
        .name("Operation summary")
        .handlesPost("/_signals/watch/{tenant}/summary")
        .with(INSTANCE, (params, body) -> new LoadOperatorSummaryRequest(params.get("tenant"), params.get("sorting"), params.get("size"), body));

    public LoadOperatorSummaryAction() {
        super(NAME, LoadOperatorSummaryRequest::new, StandardResponse::new);
    }
}
