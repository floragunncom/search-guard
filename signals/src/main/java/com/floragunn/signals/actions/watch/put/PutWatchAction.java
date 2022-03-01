package com.floragunn.signals.actions.watch.put;

import org.opensearch.action.ActionType;

public class PutWatchAction extends ActionType<PutWatchResponse> {

    public static final PutWatchAction INSTANCE = new PutWatchAction();
    public static final String NAME = "cluster:admin:searchguard:tenant:signals:watch/put";

    protected PutWatchAction() {
        super(NAME, in -> {
            PutWatchResponse response = new PutWatchResponse(in);
            return response;
        });
    }
}
