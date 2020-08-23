
package com.floragunn.searchguard.authtoken.update;

import org.elasticsearch.action.ActionType;

public class PushAuthTokenUpdateAction extends ActionType<PushAuthTokenUpdateResponse> {

    public static final PushAuthTokenUpdateAction INSTANCE = new PushAuthTokenUpdateAction();
    public static final String NAME = "cluster:admin/searchguard/auth_token/update/push";

    protected PushAuthTokenUpdateAction() {
        super(NAME, PushAuthTokenUpdateResponse::new);
    }
}
