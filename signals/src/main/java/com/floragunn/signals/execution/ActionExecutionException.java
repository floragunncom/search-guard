package com.floragunn.signals.execution;

import com.floragunn.searchsupport.jobs.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.jobs.config.validation.ValidationErrors;
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
