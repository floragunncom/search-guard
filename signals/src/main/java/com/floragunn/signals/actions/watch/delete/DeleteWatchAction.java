package com.floragunn.signals.actions.watch.delete;

import org.elasticsearch.action.ActionType;

public class DeleteWatchAction extends ActionType<DeleteWatchResponse> {

    public static final DeleteWatchAction INSTANCE = new DeleteWatchAction();
    public static final String NAME = "cluster:admin:searchguard:tenant:signals:watch/delete";

    protected DeleteWatchAction() {
        super(NAME);
    }
}
