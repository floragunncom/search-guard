package com.floragunn.signals.actions.watch.search;

import org.elasticsearch.action.ActionType;

public class SearchWatchAction extends ActionType<SearchWatchResponse> {

    public static final SearchWatchAction INSTANCE = new SearchWatchAction();
    public static final String NAME = "cluster:admin:searchguard:tenant:signals:watch/search";

    protected SearchWatchAction() {
        super(NAME);
    }
}
