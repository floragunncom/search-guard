package com.floragunn.signals.actions.admin.start_stop;

import org.elasticsearch.action.ActionType;

public class StartStopAction extends ActionType<StartStopResponse> {

    public static final StartStopAction INSTANCE = new StartStopAction();
    public static final String NAME = "cluster:admin:searchguard:signals:admin/start_stop";

    protected StartStopAction() {
        super(NAME, in -> {
            StartStopResponse response = new StartStopResponse(in);
            return response;
        });
    }

}
