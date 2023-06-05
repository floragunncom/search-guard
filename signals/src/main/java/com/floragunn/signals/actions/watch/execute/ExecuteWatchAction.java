package com.floragunn.signals.actions.watch.execute;

import org.elasticsearch.action.ActionType;

/**
 * @deprecated please use {@link ExecuteGenericWatchAction} instead. Class is still needed to preserve backwards compatibility during
 * rolling upgrade.
 */
@Deprecated
public class ExecuteWatchAction extends ActionType<ExecuteWatchResponse> {

    public static final ExecuteWatchAction INSTANCE = new ExecuteWatchAction();
    public static final String NAME = "cluster:admin:searchguard:tenant:signals:watch/execute";

    protected ExecuteWatchAction() {
        super(NAME, in -> {
            ExecuteWatchResponse response = new ExecuteWatchResponse(in);
            return response;
        });
    }
}
