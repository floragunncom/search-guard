/*
 * Copyright 2021-2022 floragunn GmbH
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

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.floragunn.searchguard.configuration.ProtectedConfigIndexService;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.PrivilegesEvaluator;
import com.floragunn.searchguard.authz.actions.Action.WellKnownAction;
import com.floragunn.searchguard.authz.actions.Action.WellKnownAction.NewResource;
import com.floragunn.searchguard.authz.actions.Action.WellKnownAction.Resource;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.indices.IndexCleanupAgent;
import com.google.common.base.Objects;

public class ResourceOwnerService implements ComponentStateProvider, ProtectedConfigIndexService.IndexReadyListener {

    public static final Setting<Integer> MAX_CHECK_RETRIES = Setting.intSetting("searchguard.resource_owner_handling.retry_owner_check.max", 1,
            Property.NodeScope, Property.Filtered);
    public static final Setting<Integer> CHECK_RETRY_DELAY = Setting.intSetting("searchguard.resource_owner_handling.retry_owner_check.delay_ms", 10,
            Property.NodeScope, Property.Filtered);
    public static final Setting<TimeValue> CLEANUP_INTERVAL = Setting.timeSetting("searchguard.resource_owner_handling.cleanup_interval",
            TimeValue.timeValueHours(1), TimeValue.timeValueMinutes(1), Property.NodeScope, Property.Filtered);
    public static final Setting<TimeValue> DEFAULT_RESOURCE_LIFETIME = Setting.timeSetting(
            "searchguard.resource_owner_handling.resource.default_lifetime", TimeValue.timeValueDays(7), TimeValue.timeValueMinutes(1),
            Property.NodeScope, Property.Filtered);
    public static final Setting<String> REFRESH_POLICY = Setting.simpleString("searchguard.resource_owner_handling.index.refresh_on_write",
            WriteRequest.RefreshPolicy.IMMEDIATE.getValue(), Property.NodeScope, Property.Filtered);

    public static final List<Setting<?>> SUPPORTED_SETTINGS = Arrays.asList(MAX_CHECK_RETRIES, CHECK_RETRY_DELAY, CLEANUP_INTERVAL,
            DEFAULT_RESOURCE_LIFETIME, REFRESH_POLICY);

    private final static Logger log = LogManager.getLogger(ResourceOwnerService.class);

    private final String index = ".searchguard_resource_owner";
    private final PrivilegedConfigClient privilegedConfigClient;
    private IndexCleanupAgent indexCleanupAgent;
    private final PrivilegesEvaluator privilegesEvaluator;

    private final int maxCheckRetries;
    private final long checkRetryDelay;
    private final TimeValue defaultResourceLifetime;
    private final WriteRequest.RefreshPolicy refreshPolicy;

    private final ComponentState componentState = new ComponentState(100, null, "resource_owner_service");

    private final AtomicBoolean indexReady = new AtomicBoolean();

    public ResourceOwnerService(Client client, ClusterService clusterService, ThreadPool threadPool,
            ProtectedConfigIndexService protectedConfigIndexService, PrivilegesEvaluator privilegesEvaluator, Settings settings) {
        this.privilegedConfigClient = PrivilegedConfigClient.adapt(client);
        this.maxCheckRetries = MAX_CHECK_RETRIES.get(settings);
        this.checkRetryDelay = CHECK_RETRY_DELAY.get(settings);
        this.defaultResourceLifetime = DEFAULT_RESOURCE_LIFETIME.get(settings);
        this.refreshPolicy = WriteRequest.RefreshPolicy.parse(REFRESH_POLICY.get(settings));
        this.privilegesEvaluator = privilegesEvaluator;

        ProtectedConfigIndexService.ConfigIndex configIndex = new ProtectedConfigIndexService.ConfigIndex(index).onIndexReady(this);
        componentState.addPart(protectedConfigIndexService.createIndex(configIndex));

        this.indexCleanupAgent = new IndexCleanupAgent(index, CLEANUP_INTERVAL.get(settings), privilegedConfigClient, clusterService, threadPool);
        componentState.addPart(this.indexCleanupAgent.getComponentState());
    }

    public void storeOwner(String resourceType, Object id, User owner, long expires, ActionListener<DocWriteResponse> actionListener) {
        if (!indexReady.get()) {
            actionListener.onFailure(new Exception("Index " + index + " not ready"));
            return;
        }

        if (log.isTraceEnabled()) {
            log.trace("storeOwner(" + resourceType + ", " + id + ", " + owner + ", " + expires + ")");
        }

        String docId = resourceType + "_" + id;
        privilegedConfigClient.prepareIndex().setIndex(index).setId(docId).setSource("user_name", owner.getName(), "expires", expires)
                .setRefreshPolicy(refreshPolicy).execute(actionListener);
    }

    public void deleteOwner(String resourceType, Object id) {
        String docId = resourceType + "_" + id;
        privilegedConfigClient.prepareDelete().setIndex(index).setId(docId).execute(new ActionListener<DeleteResponse>() {

            @Override
            public void onResponse(DeleteResponse response) {
                if (log.isTraceEnabled()) {
                    log.trace("Resource owner document deleted: " + docId + "; " + response);
                }
            }

            @Override
            public void onFailure(Exception e) {
                log.error("Error while deleting resource owner document " + docId, e);
            }
        });
    }

    public void checkOwner(String resourceType, Object id, User currentUser, ActionListener<CheckOwnerResponse> actionListener) {
        checkOwner(resourceType, id, currentUser, actionListener, 0);
    }

    private void checkOwner(String resourceType, Object id, User currentUser, ActionListener<CheckOwnerResponse> actionListener, int retry) {
        String docId = resourceType + "_" + id;

        privilegedConfigClient.prepareGet().setIndex(index).setId(docId).execute(new ActionListener<GetResponse>() {

            @Override
            public void onResponse(GetResponse response) {
                if (response.isExists()) {
                    Object storedUserName = response.getSourceAsMap().get("user_name");

                    if (log.isTraceEnabled()) {
                        log.trace("checkOwner for " + resourceType + ":" + id + ": " + storedUserName + " - " + currentUser);
                    }

                    if (isUserEqual(currentUser, storedUserName)) {
                        actionListener.onResponse(new CheckOwnerResponse(response));
                    } else {
                        actionListener.onFailure(new ElasticsearchSecurityException(
                                "Resource " + resourceType + ":" + id + " is not owned by user " + currentUser.getName(), RestStatus.FORBIDDEN));
                    }
                } else if (retry < maxCheckRetries) {
                    if (log.isDebugEnabled()) {
                        log.debug("Retrying checkOwner(" + resourceType + ":" + id + ")");
                    }

                    if (checkRetryDelay > 0) {
                        try {
                            Thread.sleep(checkRetryDelay);
                        } catch (InterruptedException e1) {
                        }
                    }

                    checkOwner(resourceType, id, currentUser, actionListener, retry + 1);
                } else {
                    if (log.isTraceEnabled()) {
                        log.trace("checkOwner for " + resourceType + ":" + id + " failed: " + response);
                    }
                    actionListener.onFailure(new ElasticsearchSecurityException(
                            "Owner information of " + resourceType + ":" + id + " could not be found", RestStatus.NOT_FOUND));
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (retry < maxCheckRetries) {
                    if (log.isDebugEnabled()) {
                        log.debug("Retrying checkOwner(" + resourceType + ":" + id + ") after " + e, e);
                    }

                    if (checkRetryDelay > 0) {
                        try {
                            Thread.sleep(checkRetryDelay);
                        } catch (InterruptedException e1) {
                        }
                    }

                    checkOwner(resourceType, id, currentUser, actionListener, retry + 1);
                } else {
                    if (log.isWarnEnabled()) {
                        log.warn("checkOwner for " + resourceType + ":" + id + " failed: ", e);
                    }

                    actionListener.onFailure(new ElasticsearchException("Checking owner of " + resourceType + ":" + id + " failed", e));
                }
            }
        });
    }

    public <Request extends ActionRequest, Response extends ActionResponse> ActionFilterChain<Request, Response> applyOwnerCheckPreAction(
            WellKnownAction<Request, ?, ?> actionConfig, PrivilegesEvaluationContext context, Request actionRequest,
            ActionListener<Response> listener, ActionFilterChain<Request, Response> chain) {

        ActionFilterChain<Request, Response> extendedChain = chain;

        for (Resource usesResource : actionConfig.getResources().getUsesResources()) {
            if (usesResource.getOwnerCheckBypassPermission() != null) {
                try {
                    if (this.privilegesEvaluator.hasClusterPermissions(usesResource.getOwnerCheckBypassPermission(), context)) {
                        continue;
                    }
                } catch (PrivilegesEvaluationException e) {
                    log.error("Error while evaluating owner check bypass permission of " + usesResource, e);
                }
            }

            Object resourceId = usesResource.getId().apply(actionRequest);

            extendedChain = new OwnerCheckPreAction<Request, Response>(usesResource, resourceId, context.getUser(), extendedChain);
        }

        return extendedChain;
    }

    public <Request extends ActionRequest, Response extends ActionResponse> ActionListener<Response> applyCreatePostAction(Request request, WellKnownAction<?, ?, ?> actionConfig, User currentUser,
            ActionListener<Response> actionListener) {

        return new ActionListener<Response>() {

            @Override
            public void onResponse(Response actionResponse) {
                NewResource newResource = actionConfig.getResources().getCreatesResource();
                Object id = newResource.getId().apply(actionResponse);

                if (log.isTraceEnabled()) {
                    log.trace("Id for new resource " + newResource + ": " + id);
                }

                if (id != null) {

                    long expiresMillis = System.currentTimeMillis() + defaultResourceLifetime.millis();

                    if (newResource.getExpiresAfter() != null) {
                        Instant expiresInstant = newResource.getExpiresAfter().apply(request, actionResponse);
                        log.debug("Resource expiration time for action '{}' is '{}'", actionConfig.name(), expiresInstant);
                        if (expiresInstant != null) {
                            expiresMillis = expiresInstant.toEpochMilli();
                        }
                    }

                    actionResponse.incRef();
                    storeOwner(newResource.getType(), id, currentUser, expiresMillis, new ActionListener<DocWriteResponse>() {

                        @Override
                        public void onResponse(DocWriteResponse indexResponse) {
                            try {
                                actionListener.onResponse(actionResponse);
                            } finally {
                                actionResponse.decRef();
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            try {
                                actionListener.onFailure(new ElasticsearchException("Failed to store owner of " + newResource.getType() + ":" + id,
                                    e));
                            } finally {
                                actionResponse.decRef();
                            }
                        }

                    });
                } else {
                    actionListener.onResponse(actionResponse);
                }

            }

            @Override
            public void onFailure(Exception e) {
                actionListener.onFailure(e);
            }
        };

    }

    public <Request extends ActionRequest, R extends ActionResponse> ActionListener<R> applyDeletePostAction(WellKnownAction<?, ?, ?> actionConfig,
            Resource resource, User currentUser, Request actionRequest, ActionListener<R> actionListener) {

        return new ActionListener<R>() {

            @Override
            public void onResponse(R actionResponse) {
                Object id = resource.getId().apply(actionRequest);

                if (id != null) {
                    deleteOwner(resource.getType(), id);
                }

                actionListener.onResponse(actionResponse);
            }

            @Override
            public void onFailure(Exception e) {
                actionListener.onFailure(e);
            }
        };

    }

    private boolean isUserEqual(User currentUser, Object storedUserName) {
        return Objects.equal(currentUser.getName(), storedUserName);
    }

    public void shutdown() {
        this.indexCleanupAgent.shutdown();
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

    @Override
    public void onIndexReady(ProtectedConfigIndexService.FailureListener failureListener) {
        indexReady.set(true);
        failureListener.onSuccess();
        this.componentState.updateStateFromParts();
    }

    static class CheckOwnerResponse {
        private GetResponse getResponse;

        CheckOwnerResponse(GetResponse getResponse) {
            this.getResponse = getResponse;
        }

        public GetResponse getGetResponse() {
            return getResponse;
        }
    }

    abstract class PreAction<Request extends ActionRequest, Response extends ActionResponse> implements ActionFilterChain<Request, Response> {
        protected final ActionFilterChain<Request, Response> next;

        PreAction(ActionFilterChain<Request, Response> next) {
            this.next = next;
        }

    }

    class OwnerCheckPreAction<Request extends ActionRequest, Response extends ActionResponse> extends PreAction<Request, Response> {
        private final Resource resource;
        private final Object resourceId;
        private final User currentUser;

        OwnerCheckPreAction(Resource resource, Object resourceId, User currentUser, ActionFilterChain<Request, Response> next) {
            super(next);
            this.currentUser = currentUser;
            this.resource = resource;
            this.resourceId = resourceId;
        }

        @Override
        public void proceed(Task task, String action, Request request, ActionListener<Response> listener) {
            checkOwner(resource.getType(), resourceId, currentUser, new ActionListener<CheckOwnerResponse>() {

                @Override
                public void onResponse(CheckOwnerResponse response) {
                    next.proceed(task, action, request, listener);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }

            });
        }

    }

}
