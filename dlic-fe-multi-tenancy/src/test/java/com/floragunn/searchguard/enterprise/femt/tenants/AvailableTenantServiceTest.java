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
package com.floragunn.searchguard.enterprise.femt.tenants;

import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.AuthorizationService;
import com.floragunn.searchguard.authz.TenantAccessMapper;
import com.floragunn.searchguard.authz.config.MultiTenancyConfigurationProvider;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;

import static com.floragunn.searchsupport.junit.ThrowableAssert.assertThatThrown;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AvailableTenantServiceTest {

    public static final String TENANT_1 = "tenant-1";
    public static final String TENANT_2 = "tenant-2";
    public static final String TENANT_3 = "tenant-3";
    public static final String TENANT_4 = "tenant-4";
    public static final String TENANT_5 = "tenant-5";
    @Mock
    private MultiTenancyConfigurationProvider configProvider;
    @Mock
    private AuthorizationService authorizationService;
    @Mock
    private ThreadPool threadPool;
    @Mock
    private TenantRepository repository;
    @Mock
    private ThreadContext threadContext;

    @Mock
    private TenantAccessMapper accessMapper;

    // under test
    private AvailableTenantService service;;

    @Before
    public void before() {
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        this.service = new AvailableTenantService(configProvider, authorizationService, threadPool, repository);
    }

    @Test
    public void shouldReturnErrorWhenUserIsNotPresentInContext() {
        when(threadContext.getTransient(ConfigConstants.SG_USER)).thenReturn(null);

        Optional<AvailableTenantData> response = service.findTenantAvailableForCurrentUser();

        assertThat(response.isPresent(), equalTo(false));
    }

    @Test
    public void shouldThrowExceptionWhenMultiTenancyIsEnabledAndUserHaveAccessToZeroTenants() {
        User user = new User("user");
        TransportAddress remoteAddress = new TransportAddress(new InetSocketAddress(8901));
        when(threadContext.getTransient(ConfigConstants.SG_USER)).thenReturn(user);
        when(threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS)).thenReturn(remoteAddress);
        when(authorizationService.getMappedRoles(user, remoteAddress)).thenReturn(ImmutableSet.of("my_role"));
        when(repository.exists(any(String[].class))).thenAnswer(new TenantExistsAnswer(true));
        when(configProvider.isMultiTenancyEnabled()).thenReturn(true);
        this.service = new AvailableTenantService(configProvider, authorizationService, threadPool, repository);
        when(configProvider.getTenantAccessMapper()).thenReturn(accessMapper);

        assertThatThrown(() -> service.findTenantAvailableForCurrentUser(), instanceOf(DefaultTenantNotFoundException.class));
    }

    @Test
    public void shouldNotThrowExceptionWhenMultiTenancyIsDisabledAndUserHaveAccessToZeroTenants() {
        User user = User.forUser("user").requestedTenant("my_tenant").build();
        when(threadContext.getTransient(ConfigConstants.SG_USER)).thenReturn(user);
        when(configProvider.isMultiTenancyEnabled()).thenReturn(false);
        this.service = new AvailableTenantService(configProvider, authorizationService, threadPool, repository);

        AvailableTenantData tenantAvailableForCurrentUser = service.findTenantAvailableForCurrentUser().get();

        assertThat(tenantAvailableForCurrentUser.multiTenancyEnabled(), equalTo(false));
        assertThat(tenantAvailableForCurrentUser.tenants(), anEmptyMap());
        assertThat(tenantAvailableForCurrentUser.defaultTenant(), nullValue());
        assertThat(tenantAvailableForCurrentUser.username(), equalTo("user"));
        assertThat(tenantAvailableForCurrentUser.userRequestedTenant(), equalTo("my_tenant"));


    }

    @Test
    public void shouldReturnInformationAboutTwoExistingTenantsWithWriteAccess() {
        User user = User.forUser("user").requestedTenant("my_tenant").build();
        TransportAddress remoteAddress = new TransportAddress(new InetSocketAddress(8901));
        when(threadContext.getTransient(ConfigConstants.SG_USER)).thenReturn(user);
        when(threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS)).thenReturn(remoteAddress);
        ImmutableSet<String> userRoles = ImmutableSet.of("my_role1", "myRole2");
        when(authorizationService.getMappedRoles(user, remoteAddress)).thenReturn(userRoles);
        when(configProvider.getTenantAccessMapper()).thenReturn(accessMapper);
        when(configProvider.isMultiTenancyEnabled()).thenReturn(true);
        when(accessMapper.mapTenantsAccess(user, userRoles)).thenReturn(ImmutableMap.of(TENANT_1, true, TENANT_2, true));
        when(repository.exists(any(String[].class))).thenAnswer(new TenantExistsAnswer(true));

        AvailableTenantData data = service.findTenantAvailableForCurrentUser().orElseThrow();

        assertThat(data.multiTenancyEnabled(), equalTo(true));
        assertThat(data.userRequestedTenant(), equalTo("my_tenant"));
        Map<String, TenantAccessData> tenants = data.tenants();
        assertThat(tenants, aMapWithSize(2));
        TenantAccessData accessData = tenants.get(TENANT_1);
        assertThat(accessData.readAccess(), equalTo(true));
        assertThat(accessData.writeAccess(), equalTo(true));
        assertThat(accessData.exists(), equalTo(true));
        accessData = tenants.get(TENANT_2);
        assertThat(accessData.readAccess(), equalTo(true));
        assertThat(accessData.writeAccess(), equalTo(true));
        assertThat(accessData.exists(), equalTo(true));
    }

    @Test
    public void shouldNotReturnInformationAboutTenantsWithReadOnlyAccessWhichDoesNotExist() {
        User user = new User("user");
        TransportAddress remoteAddress = new TransportAddress(new InetSocketAddress(8901));
        when(threadContext.getTransient(ConfigConstants.SG_USER)).thenReturn(user);
        when(threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS)).thenReturn(remoteAddress);
        ImmutableSet<String> userRoles = ImmutableSet.of("my_nice_role");
        when(authorizationService.getMappedRoles(user, remoteAddress)).thenReturn(userRoles);
        when(configProvider.getTenantAccessMapper()).thenReturn(accessMapper);
        when(configProvider.isMultiTenancyEnabled()).thenReturn(true);
        ImmutableMap<String, Boolean> tenantWriteAccess = ImmutableMap.of(TENANT_4, true, TENANT_5, false);
        when(accessMapper.mapTenantsAccess(user, userRoles)).thenReturn(tenantWriteAccess);
        when(repository.exists(any(String[].class))).thenAnswer(new TenantExistsAnswer(false));

        AvailableTenantData data = service.findTenantAvailableForCurrentUser().orElseThrow();

        assertThat(data.multiTenancyEnabled(), equalTo(true));
        assertThat(data.userRequestedTenant(), nullValue());
        Map<String, TenantAccessData> tenants = data.tenants();
        assertThat(tenants, aMapWithSize(1));
        TenantAccessData accessData = tenants.get(TENANT_4);
        assertThat(accessData.readAccess(), equalTo(true));
        assertThat(accessData.writeAccess(), equalTo(true));
        assertThat(accessData.exists(), equalTo(false));
    }

    @Test
    public void shouldReturnInformationAboutFiveVariousTenants_1() {
        User user = new User("user");
        TransportAddress remoteAddress = new TransportAddress(new InetSocketAddress(8901));
        when(threadContext.getTransient(ConfigConstants.SG_USER)).thenReturn(user);
        when(threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS)).thenReturn(remoteAddress);
        ImmutableSet<String> userRoles = ImmutableSet.of("my_nice_role");
        when(authorizationService.getMappedRoles(user, remoteAddress)).thenReturn(userRoles);
        when(configProvider.getTenantAccessMapper()).thenReturn(accessMapper);
        when(configProvider.isMultiTenancyEnabled()).thenReturn(true);
        var tenantWriteAccess = ImmutableMap.of(TENANT_1, false, TENANT_2, true, TENANT_3, true, TENANT_4, false, TENANT_5, true);
        when(accessMapper.mapTenantsAccess(user, userRoles)).thenReturn(tenantWriteAccess);
        when(repository.exists(any(String[].class))).thenReturn(ImmutableSet.of(TENANT_1, TENANT_2, TENANT_4));

        AvailableTenantData data = service.findTenantAvailableForCurrentUser().orElseThrow();

        assertThat(data.multiTenancyEnabled(), equalTo(true));
        Map<String, TenantAccessData> tenants = data.tenants();
        assertThat(tenants, aMapWithSize(5));
        TenantAccessData accessData = tenants.get(TENANT_1);
        assertThat(accessData.readAccess(), equalTo(true));
        assertThat(accessData.writeAccess(), equalTo(false));
        assertThat(accessData.exists(), equalTo(true));
        accessData = tenants.get(TENANT_2);
        assertThat(accessData.readAccess(), equalTo(true));
        assertThat(accessData.writeAccess(), equalTo(true));
        assertThat(accessData.exists(), equalTo(true));
        accessData = tenants.get(TENANT_3);
        assertThat(accessData.readAccess(), equalTo(true));
        assertThat(accessData.writeAccess(), equalTo(true));
        assertThat(accessData.exists(), equalTo(false));
        accessData = tenants.get(TENANT_4);
        assertThat(accessData.readAccess(), equalTo(true));
        assertThat(accessData.writeAccess(), equalTo(false));
        assertThat(accessData.exists(), equalTo(true));
        accessData = tenants.get(TENANT_5);
        assertThat(accessData.readAccess(), equalTo(true));
        assertThat(accessData.writeAccess(), equalTo(true));
        assertThat(accessData.exists(), equalTo(false));
    }

    @Test
    public void shouldReturnInformationAboutFiveVariousTenants_2() {
        User user = new User("user");
        TransportAddress remoteAddress = new TransportAddress(new InetSocketAddress(8901));
        when(threadContext.getTransient(ConfigConstants.SG_USER)).thenReturn(user);
        when(threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS)).thenReturn(remoteAddress);
        ImmutableSet<String> userRoles = ImmutableSet.of("my_nice_role");
        when(authorizationService.getMappedRoles(user, remoteAddress)).thenReturn(userRoles);
        when(configProvider.getTenantAccessMapper()).thenReturn(accessMapper);
        when(configProvider.isMultiTenancyEnabled()).thenReturn(true);
        var tenantWriteAccess = ImmutableMap.of(TENANT_1, false, TENANT_2, false, TENANT_3, false, TENANT_4, true, TENANT_5, true);
        when(accessMapper.mapTenantsAccess(user, userRoles)).thenReturn(tenantWriteAccess);
        when(repository.exists(any(String[].class))).thenReturn(ImmutableSet.of(TENANT_1, TENANT_2, TENANT_4));

        AvailableTenantData data = service.findTenantAvailableForCurrentUser().orElseThrow();

        assertThat(data.multiTenancyEnabled(), equalTo(true));
        Map<String, TenantAccessData> tenants = data.tenants();
        assertThat(tenants, aMapWithSize(4));
        TenantAccessData accessData = tenants.get(TENANT_1);
        assertThat(accessData.readAccess(), equalTo(true));
        assertThat(accessData.writeAccess(), equalTo(false));
        assertThat(accessData.exists(), equalTo(true));
        accessData = tenants.get(TENANT_2);
        assertThat(accessData.readAccess(), equalTo(true));
        assertThat(accessData.writeAccess(), equalTo(false));
        assertThat(accessData.exists(), equalTo(true));
        accessData = tenants.get(TENANT_4);
        assertThat(accessData.readAccess(), equalTo(true));
        assertThat(accessData.writeAccess(), equalTo(true));
        assertThat(accessData.exists(), equalTo(true));
        accessData = tenants.get(TENANT_5);
        assertThat(accessData.readAccess(), equalTo(true));
        assertThat(accessData.writeAccess(), equalTo(true));
        assertThat(accessData.exists(), equalTo(false));
    }

    private static class TenantExistsAnswer implements Answer<ImmutableSet<String>> {

        private final boolean eachTenantExists;

        public TenantExistsAnswer(boolean eachTenantExists) {
            this.eachTenantExists = eachTenantExists;
        }

        @Override
        public ImmutableSet<String> answer(InvocationOnMock invocation) {
            if (eachTenantExists) {
                ImmutableSet.Builder<String> builder = new ImmutableSet.Builder<>();
                for (int i = 0; i < invocation.getArguments().length; ++i) {
                    String tenantName = invocation.getArgument(i);
                    builder.add(tenantName);
                }
                return builder.build();
            }else {
                return ImmutableSet.empty();
            }
        }
    }
}