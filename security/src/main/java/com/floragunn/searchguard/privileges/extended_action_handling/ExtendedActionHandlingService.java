/*
 * Copyright 2021 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.floragunn.searchguard.privileges.extended_action_handling;

import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.common.settings.Settings;
import org.opensearch.tasks.Task;

import com.floragunn.searchguard.authz.actions.Action.WellKnownAction;
import com.floragunn.searchguard.authz.actions.Action.WellKnownAction.Resource;
import com.floragunn.searchguard.user.User;

public class ExtendedActionHandlingService {
    private final ResourceOwnerService resourceOwnerService;

    public ExtendedActionHandlingService(ResourceOwnerService resourceOwnerService, Settings settings) {
        this.resourceOwnerService = resourceOwnerService;
    }

    public <Request extends ActionRequest, Response extends ActionResponse> void apply(WellKnownAction<Request, ?, ?> actionConfig, User currentUser,
            Task task, final String action, Request actionRequest, ActionListener<Response> actionListener,
            ActionFilterChain<Request, Response> chain) {

        actionListener = applyPostAction(actionConfig, currentUser, task, action, actionRequest, actionListener);

        applyPreAction(actionConfig, currentUser, task, action, actionRequest, actionListener, chain);
    }

    public <Request extends ActionRequest, Response extends ActionResponse> void applyPreAction(WellKnownAction<Request, ?, ?> actionConfig,
            User currentUser, Task task, final String action, Request actionRequest, ActionListener<Response> listener,
            ActionFilterChain<Request, Response> chain) {

        ActionFilterChain<Request, Response> extendedChain = chain;

        extendedChain = resourceOwnerService.applyOwnerCheckPreAction(actionConfig, currentUser, task, action, actionRequest, listener,
                extendedChain);

        extendedChain.proceed(task, action, actionRequest, listener);
    }

    public <Request extends ActionRequest, R extends ActionResponse> ActionListener<R> applyPostAction(WellKnownAction<Request, ?, ?> actionConfig,
            User currentUser, Task task, final String action, Request actionRequest, ActionListener<R> actionListener) {

        if (actionConfig.getResources() != null) {

            if (actionConfig.getResources().getCreatesResource() != null) {
                actionListener = resourceOwnerService.applyCreatePostAction(actionConfig, currentUser, actionListener);
            }

            for (Resource resource : actionConfig.getResources().getUsesResources()) {
                if (resource.isDeleteAction()) {
                    actionListener = resourceOwnerService.applyDeletePostAction(actionConfig, resource, currentUser, task, action, actionRequest,
                            actionListener);
                }

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
