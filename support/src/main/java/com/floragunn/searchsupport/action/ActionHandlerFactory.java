package com.floragunn.searchsupport.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;

public class ActionHandlerFactory {

    public static <Request extends ActionRequest, Response extends ActionResponse> ActionHandler<Request, Response> actionHandler(ActionType<Response> action, Class<? extends TransportAction<Request, Response>> transportAction) {
        return new ActionHandler<>(action, transportAction);
    }


}
