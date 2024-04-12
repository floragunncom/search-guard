package com.floragunn.signals.actions.settings.get;

import org.elasticsearch.action.ActionType;

public class GetSettingsAction extends ActionType<GetSettingsResponse> {

    public static final GetSettingsAction INSTANCE = new GetSettingsAction();
    public static final String NAME = "cluster:admin:searchguard:signals:settings/get";

    protected GetSettingsAction() {
        super(NAME);
    }
}
