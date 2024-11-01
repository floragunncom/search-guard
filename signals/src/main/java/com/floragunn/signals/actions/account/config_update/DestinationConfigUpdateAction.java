package com.floragunn.signals.actions.account.config_update;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.Client;

public class DestinationConfigUpdateAction extends ActionType<DestinationConfigUpdateResponse> {
    private final static Logger log = LogManager.getLogger(DestinationConfigUpdateAction.class);

    public static final DestinationConfigUpdateAction INSTANCE = new DestinationConfigUpdateAction();
    public static final String NAME = "cluster:admin:searchguard:signals:destination/update"; //not tenant related

    protected DestinationConfigUpdateAction() {
        super(NAME, in -> {
            DestinationConfigUpdateResponse response = new DestinationConfigUpdateResponse(in);
            return response;
        });
    }

    public static void send(Client client) {
        client.execute(DestinationConfigUpdateAction.INSTANCE, new DestinationConfigUpdateRequest(),
                new ActionListener<DestinationConfigUpdateResponse>() {

                    @Override
                    public void onResponse(DestinationConfigUpdateResponse response) {
                        log.info("Result of destination config update:\n" + response);

                    }

                    @Override
                    public void onFailure(Exception e) {
                        log.error("Scheduler config update failed", e);
                    }
                });
    }

}
