package com.floragunn.searchsupport.jobs;

import com.floragunn.searchguard.SearchGuardModule;
import com.floragunn.searchsupport.jobs.actions.CheckForExecutingTriggerAction;
import com.floragunn.searchsupport.jobs.actions.SchedulerConfigUpdateAction;
import com.floragunn.searchsupport.jobs.actions.TransportCheckForExecutingTriggerAction;
import com.floragunn.searchsupport.jobs.actions.TransportSchedulerConfigUpdateAction;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.plugins.ActionPlugin;

import java.util.List;

public class SchedulerModule implements SearchGuardModule {
    @Override
    public List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
                new ActionPlugin.ActionHandler<>(CheckForExecutingTriggerAction.INSTANCE, TransportCheckForExecutingTriggerAction.class),
                new ActionPlugin.ActionHandler<>(SchedulerConfigUpdateAction.INSTANCE, TransportSchedulerConfigUpdateAction.class)
        );
    }
}
