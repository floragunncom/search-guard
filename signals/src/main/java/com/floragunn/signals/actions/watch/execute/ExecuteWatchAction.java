package com.floragunn.signals.actions.watch.execute;

import org.elasticsearch.action.ActionType;

public class ExecuteWatchAction extends ActionType<ExecuteWatchResponse> {

    public static final ExecuteWatchAction INSTANCE = new ExecuteWatchAction();
    public static final String NAME = "cluster:admin:searchguard:tenant:signals:watch/execute";

    protected ExecuteWatchAction() {
        super(NAME);
    }
}
