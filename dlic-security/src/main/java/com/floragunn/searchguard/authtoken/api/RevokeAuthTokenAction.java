package com.floragunn.searchguard.authtoken.api;

import org.elasticsearch.action.ActionType;

public class RevokeAuthTokenAction extends ActionType<RevokeAuthTokenResponse> {

    public static final RevokeAuthTokenAction INSTANCE = new RevokeAuthTokenAction();
    public static final String NAME = "cluster:admin:searchguard:authtoken/revoke";

    protected RevokeAuthTokenAction() {
        super(NAME, in -> {
            RevokeAuthTokenResponse response = new RevokeAuthTokenResponse(in);
            return response;
        });
    }
}
