package com.floragunn.searchguard.authtoken.api;

import org.elasticsearch.action.ActionType;

public class SearchAuthTokensAction extends ActionType<SearchAuthTokensResponse> {

    public static final SearchAuthTokensAction INSTANCE = new SearchAuthTokensAction();
    public static final String NAME = "cluster:admin:searchguard:authtoken/_own/search";
    public static final String NAME_ALL = NAME.replace("/_own/", "/_all/");

    protected SearchAuthTokensAction() {
        super(NAME, in -> {
            SearchAuthTokensResponse response = new SearchAuthTokensResponse(in);
            return response;
        });
    }
}
