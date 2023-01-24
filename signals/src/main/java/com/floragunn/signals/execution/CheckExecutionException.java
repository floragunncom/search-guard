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
package com.floragunn.signals.execution;

import com.floragunn.signals.watch.checks.Check;
import com.floragunn.signals.watch.result.ErrorInfo;

public class CheckExecutionException extends WatchOperationExecutionException {

    private static final long serialVersionUID = 9090226882137384236L;

    private final String checkId;

    public CheckExecutionException(Check check, String message, Throwable cause) {
        super(message, cause);
        this.checkId = check.toString();
    }

    public CheckExecutionException(Check check, String message) {
        super(message);
        this.checkId = check.toString();
    }

    public String getCheckId() {
        return checkId;
    }

    public ErrorInfo toErrorInfo() {
        ErrorInfo result = super.toErrorInfo();

        result.setCheck(checkId);

        return result;
    }

}
