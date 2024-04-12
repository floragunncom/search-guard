package com.floragunn.signals.actions.account.search;

import org.elasticsearch.action.ActionType;

public class SearchAccountAction extends ActionType<SearchAccountResponse> {

    public static final SearchAccountAction INSTANCE = new SearchAccountAction();
    public static final String NAME = "cluster:admin:searchguard:signals:account/search";

    protected SearchAccountAction() {
        super(NAME);
    }
}
