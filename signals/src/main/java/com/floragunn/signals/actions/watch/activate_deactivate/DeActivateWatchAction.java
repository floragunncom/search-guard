package com.floragunn.signals.actions.watch.activate_deactivate;

import org.elasticsearch.action.ActionType;

public class DeActivateWatchAction extends ActionType<DeActivateWatchResponse> {

    public static final DeActivateWatchAction INSTANCE = new DeActivateWatchAction();
    public static final String NAME = "cluster:admin:searchguard:tenant:signals:watch/activate_deactivate";

    protected DeActivateWatchAction() {
        super(NAME);
    }

}
