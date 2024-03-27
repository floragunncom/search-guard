/*
 * Copyright 2023-2024 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */
package com.floragunn.searchguard.enterprise.femt;

import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.PrivilegesEvaluationResult;
import com.floragunn.searchguard.authz.SyncAuthorizationFilter;
import com.floragunn.searchguard.authz.TenantManager;
import com.floragunn.searchguard.authz.actions.Action;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector.ResolvedIndices;
import com.floragunn.searchguard.authz.actions.Actions;
import com.floragunn.searchguard.enterprise.femt.request.handler.RequestHandler;
import com.floragunn.searchguard.enterprise.femt.request.handler.RequestHandlerFactory;
import com.floragunn.searchguard.user.User;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.IndicesService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Optional;

import static com.floragunn.searchguard.authz.PrivilegesEvaluationResult.INSUFFICIENT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MultiTenancyAuthorizationFilterTest {

    private static final Logger log = LogManager.getLogger(MultiTenancyAuthorizationFilterTest.class);
    public static final String PRIVATE_TENANT_HEADER_VALUE = "__user__";
    public static final String INTERNAL_TEST_USER_1_PRIVATE_TENANT_NAME = "-66146748_testusername1";
    public static final String HR_TENANT_NAME = "hr_tenant_name";
    public static final String IT_TENANT_NAME = "it_tenant_name";
    public static final String FRONTEND_MAIN_INDEX = ".kibana_8.9.0_001";
    public static final String TEST_USER_NAME_1 = "test_user_name_1";
    public static final String TEST_USER_NAME_2 = "test_user_name_2";
    public static final String INTERNAL_HR_TENANT_NAME = "1140937035_hrtenantname";
    public static final String INTERNAL_IT_TENANT_NAME = "48572396_ittenantname";

    @Mock
    private FeMultiTenancyConfig config;
    @Mock
    private RoleBasedTenantAuthorization tenantAuthorization;
    @Mock
    private TenantManager tenantManager;
    @Mock
    private ThreadContext threadContext;
    @Mock
    private RequestHandlerFactory handlerFactory;
    @Mock
    private ClusterService clusterServices;
    @Mock
    private IndicesService indicesService;
    @Mock
    private ActionListener<?> listener;
    @Mock
    private PrivilegesEvaluationContext context;
    @Mock
    private ActionRequestIntrospector.ActionRequestInfo actionRequestInfo;
    @Mock
    private User user;
    @Mock
    private ResolvedIndices resolvedIndices;
    @Mock
    private Action action;
    @Mock
    private RequestHandler<ActionRequest> actionHandler;

    private MultiTenancyAuthorizationFilter filter;

    @Before
    public void before() {
        when(config.getIndex()).thenReturn(".kibana");
        when(config.isEnabled()).thenReturn(true);
        when(context.getUser()).thenReturn(user);
        when(config.getServerUsername()).thenReturn("frontend_server_user");
        when(config.isPrivateTenantEnabled()).thenReturn(false);
        this.filter = new MultiTenancyAuthorizationFilter(config, tenantAuthorization, tenantManager, new Actions(null),
            threadContext, null, handlerFactory, clusterServices, indicesService);
    }

    @Test
    public void shouldAccessPrivateTenant_privateTenantEnabled() {
        when(config.isPrivateTenantEnabled()).thenReturn(true);
        MultiTenancyAuthorizationFilter filter = new MultiTenancyAuthorizationFilter(config, tenantAuthorization, tenantManager, new Actions(null),
                threadContext, null, handlerFactory, clusterServices, indicesService);
        when(context.getRequestInfo()).thenReturn(actionRequestInfo);
        when(actionRequestInfo.getResolvedIndices()).thenReturn(resolvedIndices);
        when(resolvedIndices.isLocalAll()).thenReturn(false);
        GetRequest request = new GetRequest(FRONTEND_MAIN_INDEX, "space:default");
        when(context.getRequest()).thenReturn(request);
        when(context.getAction()).thenReturn(action);
        when(action.name()).thenReturn("indices:data/read/search");
        when(user.getName()).thenReturn(TEST_USER_NAME_1);
        when(user.getRequestedTenant()).thenReturn(PRIVATE_TENANT_HEADER_VALUE);
        when(tenantManager.isTenantHeaderValid(PRIVATE_TENANT_HEADER_VALUE)).thenReturn(true);
        when(tenantManager.isUserTenantHeader(anyString())).thenCallRealMethod();
        when(tenantManager.toInternalTenantName(context.getUser())).thenCallRealMethod();
        when(handlerFactory.requestHandlerFor(request)).thenReturn(Optional.of(actionHandler));
        when(actionHandler.handle(same(context), eq(INTERNAL_TEST_USER_1_PRIVATE_TENANT_NAME), same(request), same(listener))).thenReturn(SyncAuthorizationFilter.Result.OK);

        SyncAuthorizationFilter.Result result = filter.apply(context, listener);

        log.info("Filter response {}", result);
        assertThat(result.getStatus(), equalTo(SyncAuthorizationFilter.Result.Status.OK));
        verify(actionHandler).handle(same(context), eq(INTERNAL_TEST_USER_1_PRIVATE_TENANT_NAME), same(request), same(listener));
        verifyNoInteractions(listener);
    }

    @Test
    public void shouldNotAccessPrivateTenant() {
        when(context.getRequestInfo()).thenReturn(actionRequestInfo);
        when(actionRequestInfo.getResolvedIndices()).thenReturn(resolvedIndices);
        when(resolvedIndices.isLocalAll()).thenReturn(false);
        GetRequest request = new GetRequest(FRONTEND_MAIN_INDEX, "space:default");
        when(context.getRequest()).thenReturn(request);
        when(context.getAction()).thenReturn(action);
        when(action.name()).thenReturn("indices:data/read/search");
        when(user.getName()).thenReturn(TEST_USER_NAME_1);
        when(user.getRequestedTenant()).thenReturn(PRIVATE_TENANT_HEADER_VALUE);
        when(tenantManager.isTenantHeaderValid(PRIVATE_TENANT_HEADER_VALUE)).thenReturn(true);
        when(tenantManager.isUserTenantHeader(anyString())).thenCallRealMethod();

        SyncAuthorizationFilter.Result result = filter.apply(context, listener);

        log.info("Filter response {}", result);
        assertThat(result.getStatus(), equalTo(SyncAuthorizationFilter.Result.Status.DENIED));
        verifyNoInteractions(listener);
        verifyNoInteractions(handlerFactory);
        verifyNoInteractions(tenantAuthorization);
    }

    @Test
    public void shouldBePermittedToPerformWriteOperationOnHrTenant() throws PrivilegesEvaluationException {
        when(context.getRequestInfo()).thenReturn(actionRequestInfo);
        when(actionRequestInfo.getResolvedIndices()).thenReturn(resolvedIndices);
        when(resolvedIndices.isLocalAll()).thenReturn(false);
        IndexRequest request = new IndexRequest().index(FRONTEND_MAIN_INDEX);
        when(context.getRequest()).thenReturn(request);
        when(context.getAction()).thenReturn(action);
        when(action.name()).thenReturn("indices:data/write/index");
        when(user.getName()).thenReturn(TEST_USER_NAME_2);
        when(user.getRequestedTenant()).thenReturn(HR_TENANT_NAME);
        when(tenantManager.isTenantHeaderValid(HR_TENANT_NAME)).thenReturn(true);
        when(tenantManager.isUserTenantHeader(anyString())).thenCallRealMethod();
        when(tenantManager.toInternalTenantName(context.getUser())).thenCallRealMethod();
        when(handlerFactory.requestHandlerFor(request)).thenReturn(Optional.of(actionHandler));
        when(actionHandler.handle(same(context), eq(INTERNAL_HR_TENANT_NAME), same(request), same(listener))).thenReturn(SyncAuthorizationFilter.Result.OK);
        Actions actions = new Actions(null);
        Action readAction = KibanaActionsProvider.getKibanaReadAction(actions);
        Action writeAction = KibanaActionsProvider.getKibanaWriteAction(actions);
        when(tenantAuthorization.hasTenantPermission(context, readAction, HR_TENANT_NAME)).thenReturn(PrivilegesEvaluationResult.OK);
        when(tenantAuthorization.hasTenantPermission(context, writeAction, HR_TENANT_NAME)).thenReturn(PrivilegesEvaluationResult.OK);

        SyncAuthorizationFilter.Result result = filter.apply(context, listener);

        log.info("Filter response {}", result);
        assertThat(result.getStatus(), equalTo(SyncAuthorizationFilter.Result.Status.OK));
        verify(actionHandler).handle(same(context), eq(INTERNAL_HR_TENANT_NAME), same(request), same(listener));
        verifyNoInteractions(listener);
        verify(tenantAuthorization).hasTenantPermission(context, readAction, HR_TENANT_NAME);
        verify(tenantAuthorization).hasTenantPermission(context, writeAction, HR_TENANT_NAME);
    }

    @Test
    public void shouldNotBePermittedToPerformWriteOperationOnHrTenant() throws PrivilegesEvaluationException {
        when(context.getRequestInfo()).thenReturn(actionRequestInfo);
        when(actionRequestInfo.getResolvedIndices()).thenReturn(resolvedIndices);
        when(resolvedIndices.isLocalAll()).thenReturn(false);
        IndexRequest request = new IndexRequest().index(FRONTEND_MAIN_INDEX);
        when(context.getRequest()).thenReturn(request);
        when(context.getAction()).thenReturn(action);
        when(action.name()).thenReturn(IndexAction.NAME);
        when(user.getName()).thenReturn(TEST_USER_NAME_2);
        when(user.getRequestedTenant()).thenReturn(HR_TENANT_NAME);
        when(tenantManager.isTenantHeaderValid(HR_TENANT_NAME)).thenReturn(true);
        when(tenantManager.isUserTenantHeader(anyString())).thenCallRealMethod();
        Actions actions = new Actions(null);
        Action readAction = KibanaActionsProvider.getKibanaReadAction(actions);
        Action writeAction = KibanaActionsProvider.getKibanaWriteAction(actions);
        when(tenantAuthorization.hasTenantPermission(context, readAction, HR_TENANT_NAME)).thenReturn(PrivilegesEvaluationResult.OK);
        when(tenantAuthorization.hasTenantPermission(context, writeAction, HR_TENANT_NAME)).thenReturn(INSUFFICIENT);

        SyncAuthorizationFilter.Result result = filter.apply(context, listener);

        log.info("Filter response {}", result);
        assertThat(result.getStatus(), equalTo(SyncAuthorizationFilter.Result.Status.DENIED));
        verifyNoInteractions(listener);
        verifyNoInteractions(handlerFactory);
        verify(tenantAuthorization).hasTenantPermission(context, readAction, HR_TENANT_NAME);
        verify(tenantAuthorization).hasTenantPermission(context, writeAction, HR_TENANT_NAME);
    }

    @Test
    public void shouldBePermittedToPerformReadOnlyOperationOnItTenant() throws PrivilegesEvaluationException {
        when(context.getRequestInfo()).thenReturn(actionRequestInfo);
        when(actionRequestInfo.getResolvedIndices()).thenReturn(resolvedIndices);
        when(resolvedIndices.isLocalAll()).thenReturn(false);
        GetRequest request = new GetRequest(FRONTEND_MAIN_INDEX, "document_id");
        when(context.getRequest()).thenReturn(request);
        when(context.getAction()).thenReturn(action);
        when(action.name()).thenReturn(GetAction.NAME);
        when(user.getName()).thenReturn(TEST_USER_NAME_2);
        when(user.getRequestedTenant()).thenReturn(IT_TENANT_NAME);
        when(tenantManager.isTenantHeaderValid(IT_TENANT_NAME)).thenReturn(true);
        when(tenantManager.isUserTenantHeader(anyString())).thenCallRealMethod();
        when(tenantManager.toInternalTenantName(context.getUser())).thenCallRealMethod();
        when(handlerFactory.requestHandlerFor(request)).thenReturn(Optional.of(actionHandler));
        when(actionHandler.handle(same(context), eq(INTERNAL_IT_TENANT_NAME), same(request), same(listener))).thenReturn(SyncAuthorizationFilter.Result.OK);
        Actions actions = new Actions(null);
        Action readAction = KibanaActionsProvider.getKibanaReadAction(actions);
        Action writeAction = KibanaActionsProvider.getKibanaWriteAction(actions);
        when(tenantAuthorization.hasTenantPermission(context, readAction, IT_TENANT_NAME)).thenReturn(PrivilegesEvaluationResult.OK);
        when(tenantAuthorization.hasTenantPermission(context, writeAction, IT_TENANT_NAME)).thenReturn(INSUFFICIENT);

        SyncAuthorizationFilter.Result result = filter.apply(context, listener);

        log.info("Filter response {}", result);
        assertThat(result.getStatus(), equalTo(SyncAuthorizationFilter.Result.Status.OK));
        verify(actionHandler).handle(same(context), eq(INTERNAL_IT_TENANT_NAME), same(request), same(listener));
        verifyNoInteractions(listener);
        verify(tenantAuthorization).hasTenantPermission(context, readAction, IT_TENANT_NAME);
        verify(tenantAuthorization).hasTenantPermission(context, writeAction, IT_TENANT_NAME);
    }

    @Test
    public void shouldNotBePermittedToPerformDeleteIndexOperationOnItTenantForReadOnlyUser() throws PrivilegesEvaluationException {
        when(context.getRequestInfo()).thenReturn(actionRequestInfo);
        when(actionRequestInfo.getResolvedIndices()).thenReturn(resolvedIndices);
        when(resolvedIndices.isLocalAll()).thenReturn(false);
        DeleteIndexRequest request = new DeleteIndexRequest(FRONTEND_MAIN_INDEX);
        when(context.getRequest()).thenReturn(request);
        when(context.getAction()).thenReturn(action);
        when(action.name()).thenReturn(DeleteIndexAction.NAME);
        when(user.getName()).thenReturn(TEST_USER_NAME_2);
        when(user.getRequestedTenant()).thenReturn(IT_TENANT_NAME);
        when(tenantManager.isTenantHeaderValid(IT_TENANT_NAME)).thenReturn(true);
        when(tenantManager.isUserTenantHeader(anyString())).thenCallRealMethod();
        Actions actions = new Actions(null);
        Action readAction = KibanaActionsProvider.getKibanaReadAction(actions);
        Action writeAction = KibanaActionsProvider.getKibanaWriteAction(actions);
        when(tenantAuthorization.hasTenantPermission(context, readAction, IT_TENANT_NAME)).thenReturn(PrivilegesEvaluationResult.OK);
        when(tenantAuthorization.hasTenantPermission(context, writeAction, IT_TENANT_NAME)).thenReturn(INSUFFICIENT);

        SyncAuthorizationFilter.Result result = filter.apply(context, listener);

        log.info("Filter response {}", result);
        assertThat(result.getStatus(), equalTo(SyncAuthorizationFilter.Result.Status.DENIED));
        verifyNoInteractions(listener);
        verifyNoInteractions(handlerFactory);
        verify(tenantAuthorization).hasTenantPermission(context, readAction, IT_TENANT_NAME);
        verify(tenantAuthorization).hasTenantPermission(context, writeAction, IT_TENANT_NAME);
    }

}