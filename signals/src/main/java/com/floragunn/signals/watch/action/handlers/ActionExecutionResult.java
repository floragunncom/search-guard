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
package com.floragunn.signals.watch.action.handlers;

public class ActionExecutionResult {
    private String request;

    public ActionExecutionResult(Object request) {
        if (request != null) {
            this.request = request.toString();
        }
    }

    public ActionExecutionResult(String request) {
        this.request = request;
    }

    public ActionExecutionResult() {

    }

    public String getRequest() {
        return request;
    }
}
