package com.floragunn.searchguard.privileges.extended_action_handling;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;

import com.floragunn.searchguard.privileges.extended_action_handling.ActionConfig.Resource;
import com.floragunn.searchguard.user.User;

public class ExtendedActionHandlingService {
    private final ResourceOwnerService resourceOwnerService;

    public ExtendedActionHandlingService(ResourceOwnerService resourceOwnerService, Settings settings) {
        this.resourceOwnerService = resourceOwnerService;
    }

    public <Request extends ActionRequest, Response extends ActionResponse> void apply(ActionConfig actionConfig, User currentUser, Task task,
            final String action, Request actionRequest, ActionListener<Response> actionListener, ActionFilterChain<Request, Response> chain) {

        actionListener = applyPostAction(actionConfig, currentUser, task, action, actionRequest, actionListener);

        applyPreAction(actionConfig, currentUser, task, action, actionRequest, actionListener, chain);
    }

    public <Request extends ActionRequest, Response extends ActionResponse> void applyPreAction(ActionConfig actionConfig, User currentUser,
            Task task, final String action, Request actionRequest, ActionListener<Response> listener, ActionFilterChain<Request, Response> chain) {

        ActionFilterChain<Request, Response> extendedChain = chain;

        for (ActionConfig.RequestPropertyModifier<?> requestPropertyModifier : actionConfig.getRequestProperyModifiers()) {
            requestPropertyModifier.apply(actionRequest);
        }

        extendedChain = resourceOwnerService.applyOwnerCheckPreAction(actionConfig, currentUser, task, action, actionRequest, listener,
                extendedChain);

        extendedChain.proceed(task, action, actionRequest, listener);
    }

    public <Request extends ActionRequest, R extends ActionResponse> ActionListener<R> applyPostAction(ActionConfig actionConfig, User currentUser,
            Task task, final String action, Request actionRequest, ActionListener<R> actionListener) {

        if (actionConfig.getCreatesResource() != null) {
            actionListener = resourceOwnerService.applyCreatePostAction(actionConfig, currentUser, actionListener);
        }

        for (Resource resource : actionConfig.getUsesResources()) {
            if (resource.isDeleteAction()) {
                actionListener = resourceOwnerService.applyDeletePostAction(actionConfig, resource, currentUser, task, action, actionRequest,
                        actionListener);
            }
        }

        return actionListener;

    }

    public abstract class PreAction<Request extends ActionRequest, Response extends ActionResponse> implements ActionFilterChain<Request, Response> {
        protected final ActionFilterChain<Request, Response> next;

        PreAction(ActionFilterChain<Request, Response> next) {
            this.next = next;
        }

    }

}
