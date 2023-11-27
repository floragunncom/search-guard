package com.floragunn.searchguard.enterprise.femt;

import com.floragunn.searchguard.authz.actions.Action;
import com.floragunn.searchguard.authz.actions.Actions;

public class KibanaActionsProvider {

    private static final String KIBANA_WRITE_ACTION_NAME = "kibana:saved_objects/_/write";
    private static final String KIBANA_READ_ACTION_NAME = "kibana:saved_objects/_/read";

    private KibanaActionsProvider() {

    }

    public static Action getKibanaWriteAction(Actions actions) {
        return actions.get(KIBANA_WRITE_ACTION_NAME);
    }

    public static Action getKibanaReadAction(Actions actions) {
        return actions.get(KIBANA_READ_ACTION_NAME);
    }

}
