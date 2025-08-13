package com.floragunn.searchsupport.jobs.actions;

import java.util.Arrays;
import java.util.List;

import org.elasticsearch.plugins.ActionPlugin.ActionHandler;

import static com.floragunn.searchsupport.action.ActionHandlerFactory.actionHandler;

public class SchedulerActions {
    public static List<ActionHandler> getActions() {
        return Arrays.asList(actionHandler(SchedulerConfigUpdateAction.INSTANCE, TransportSchedulerConfigUpdateAction.class),
                actionHandler(CheckForExecutingTriggerAction.INSTANCE, TransportCheckForExecutingTriggerAction.class));
    }
}
