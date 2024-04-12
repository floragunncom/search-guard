package com.floragunn.signals.actions.settings.update;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.Client;

public class SettingsUpdateAction extends ActionType<SettingsUpdateResponse> {
    private final static Logger log = LogManager.getLogger(SettingsUpdateAction.class);

    public static final SettingsUpdateAction INSTANCE = new SettingsUpdateAction();
    public static final String NAME = "cluster:admin:searchguard:signals:settings/update";

    protected SettingsUpdateAction() {
        super(NAME);
    }

    public static void send(Client client) {
        client.execute(SettingsUpdateAction.INSTANCE, new SettingsUpdateRequest(), new ActionListener<SettingsUpdateResponse>() {

            @Override
            public void onResponse(SettingsUpdateResponse response) {
                log.info("Result of settings update:\n" + response);

            }

            @Override
            public void onFailure(Exception e) {
                log.error("settings update failed", e);
            }
        });
    }

}
