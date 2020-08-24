package com.floragunn.searchguard.authtoken.api;

import org.elasticsearch.action.ActionType;

public class RevokeAuthTokenAction extends ActionType<RevokeAuthTokenResponse> {

    public static final RevokeAuthTokenAction INSTANCE = new RevokeAuthTokenAction();
    public static final String NAME = "cluster:admin:searchguard:authtoken/_own/revoke";
    public static final String NAME_ALL = NAME.replace("/_own/", "/_all/");

    protected RevokeAuthTokenAction() {
        super(NAME, in -> {
            RevokeAuthTokenResponse response = new RevokeAuthTokenResponse(in);
            return response;
        });
    }
}
