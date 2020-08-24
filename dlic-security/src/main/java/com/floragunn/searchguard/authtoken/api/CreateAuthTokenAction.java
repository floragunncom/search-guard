package com.floragunn.searchguard.authtoken.api;

import org.elasticsearch.action.ActionType;

public class CreateAuthTokenAction extends ActionType<CreateAuthTokenResponse> {

    public static final CreateAuthTokenAction INSTANCE = new CreateAuthTokenAction();
    public static final String NAME = "cluster:admin:searchguard:authtoken/create";

    protected CreateAuthTokenAction() {
        super(NAME, in -> {
            CreateAuthTokenResponse response = new CreateAuthTokenResponse(in);
            return response;
        });
    }
}
