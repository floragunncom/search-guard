package com.floragunn.signals.watch.action.invokers;

import java.util.Collections;

import com.floragunn.signals.watch.action.handlers.ActionHandler;

public class AutoResolveAction extends ResolveAction {

    public AutoResolveAction(AlertAction alertAction, ActionHandler handler) {
        super("__resolve_" + alertAction.getName(), handler, alertAction.getSeverityLevels(), Collections.emptyList());
    }

}
