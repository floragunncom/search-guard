package com.floragunn.searchsupport.jobs.actions;

import java.util.Arrays;
import java.util.List;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;

public class SchedulerActions {
    public static List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(new ActionHandler<>(SchedulerConfigUpdateAction.INSTANCE, TransportSchedulerConfigUpdateAction.class),
                new ActionHandler<>(CheckForExecutingTriggerAction.INSTANCE, TransportCheckForExecutingTriggerAction.class));
    }
}
