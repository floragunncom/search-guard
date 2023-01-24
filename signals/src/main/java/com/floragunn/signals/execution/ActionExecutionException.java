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

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.signals.watch.action.handlers.ActionHandler;

public class ActionExecutionException extends WatchOperationExecutionException {

    private static final long serialVersionUID = 3309028338659339298L;

    private final String actionId;

    public ActionExecutionException(ActionHandler action, String message, Throwable cause) {
        super(message, cause);
        this.actionId = action != null ? action.getName() : null;
    }

    public ActionExecutionException(ActionHandler action, Throwable cause) {
        super(cause.getMessage(), cause);
        this.actionId = action != null ? action.getName() : null;
    }

    public ActionExecutionException(ActionHandler action, String message) {
        super(message);
        this.actionId = action != null ? action.getName() : null;
    }

    public ActionExecutionException(ActionHandler action, String message, ValidationErrors validationErrors) {
        this(action, message, new ConfigValidationException(validationErrors));
    }

    public String getActionId() {
        return actionId;
    }
}
