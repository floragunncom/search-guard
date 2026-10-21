/*
 * Copyright 2023 by floragunn GmbH - All rights reserved
 * 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 
 * This software is free of charge for non-commercial and academic use. 
 * For commercial use in a production environment you have to obtain a license 
 * from https://floragunn.com
 * 
 */

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
