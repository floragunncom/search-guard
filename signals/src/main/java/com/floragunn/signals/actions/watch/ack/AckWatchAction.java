
package com.floragunn.signals.actions.watch.ack;

import org.elasticsearch.action.ActionType;

public class AckWatchAction extends ActionType<AckWatchResponse> {
    public static final AckWatchAction INSTANCE = new AckWatchAction();
    public static final String NAME = "cluster:admin:searchguard:tenant:signals:watch/ack";

    protected AckWatchAction() {
        super(NAME, in -> {
            AckWatchResponse response = new AckWatchResponse(in);
            return response;
        });
    }
}
