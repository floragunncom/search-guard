
package com.floragunn.signals.actions.watch.ackandget;

import org.elasticsearch.action.ActionType;

public class AckAndGetWatchAction extends ActionType<AckAndGetWatchResponse> {
    public static final AckAndGetWatchAction INSTANCE = new AckAndGetWatchAction();
    public static final String NAME = "cluster:admin:searchguard:tenant:signals:watch/ack_and_get";

    protected AckAndGetWatchAction() {
        super(NAME);
    }
}
