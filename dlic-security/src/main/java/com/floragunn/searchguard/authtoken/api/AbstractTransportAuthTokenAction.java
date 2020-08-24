package com.floragunn.searchguard.authtoken.api;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.transport.TransportService;

import com.floragunn.searchguard.privileges.PrivilegesEvaluator;
import com.floragunn.searchguard.user.User;

abstract class AbstractTransportAuthTokenAction<Request extends ActionRequest, Response extends ActionResponse>
        extends HandledTransportAction<Request, Response> {
    private final PrivilegesEvaluator privilegesEvaluator;
    private final String allActionName;

    public AbstractTransportAuthTokenAction(String actionName, TransportService transportService, ActionFilters actionFilters,
            Reader<Request> requestReader, PrivilegesEvaluator privilegesEvaluator) {
        super(actionName, transportService, actionFilters, requestReader);
        this.privilegesEvaluator = privilegesEvaluator;
        this.allActionName = getAllActionName(actionName);
    }

    protected boolean isAllowedToAccessAll(User user) {
        return this.privilegesEvaluator.hasClusterPermission(user, this.allActionName);
    }

    protected static String getAllActionName(String actionName) {
        return actionName.replace("/_own/", "/_all/");
    }

}
