/*
 * Copyright 2025 floragunn GmbH
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

package com.floragunn.searchguard.internalauthtoken;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.PrivilegesEvaluator;
import com.floragunn.searchguard.authz.actions.Actions;
import com.floragunn.searchguard.authz.config.ActionGroup;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.test.TestSgConfig;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InternalAuthTokenProviderTest {
    static final String SIGNING_KEY = "8me7E2GunuIMaeBCbwl+7Le4TzydK7Sv2/kr0p4EVcqisyT3U5qkExBYVMAycYfYyN3Q/e8YYrWd2kZKWVkCJg==";
    static final String JWT_WITH_DLS_ROLE = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ0ZXN0X3VzZXIiLCJhdWQiOiJ0ZXN0X2F1ZGllbmNlIiwibmJmIjoxNzYxMTU2LCJzZ19pIjoibiIsInNnX3JvbGVzIjp7ImRsc19yb2xlIjp7ImNsdXN0ZXJfcGVybWlzc2lvbnMiOltdLCJpbmRleF9wZXJtaXNzaW9ucyI6W3siaW5kZXhfcGF0dGVybnMiOlsibXlfaW5kZXgqIl0sImFsbG93ZWRfYWN0aW9ucyI6WyIqIl0sImRscyI6IntcIm1hdGNoX2FsbFwiOnt9fSJ9XSwiYWxpYXNfcGVybWlzc2lvbnMiOltdLCJkYXRhX3N0cmVhbV9wZXJtaXNzaW9ucyI6W10sInRlbmFudF9wZXJtaXNzaW9ucyI6W10sImV4Y2x1ZGVfY2x1c3Rlcl9wZXJtaXNzaW9ucyI6W119fX0.DNYCxLRQPR1zoYBMqDUcNv2RRGFPN8ak6EC9cyJCADikehvtcGCv6GLnh895n_we_HuMLrwedOM_9pi_LHkQSA";

    static final PrivilegesEvaluator privilegesEvaluator = mock(PrivilegesEvaluator.class);
    static {
        when(privilegesEvaluator.getActionGroups()).thenReturn(ActionGroup.FlattenedIndex.EMPTY);
        when(privilegesEvaluator.getAllConfiguredTenantNames()).thenReturn(ImmutableSet.empty());
    }
    static final Actions actions = new Actions(null);
    static final SgDynamicConfiguration<Role> dlsRolesConfig;
    static {
        try {
            dlsRolesConfig = TestSgConfig.Role.toActualRole(getMockContextWithMatchAllQueryParser(),
                    new TestSgConfig.Role("dls_role").indexPermissions("*").dls("{\"match_all\":{}}").on("my_index*"));
        } catch (ConfigValidationException e) {
            throw new RuntimeException(e);
        }
    }

    private static ConfigurationRepository getMockConfigurationRepository(SgDynamicConfiguration<Role> dlsRolesConfig, ConfigurationRepository.Context context) {
        ConfigurationRepository configurationRepository = mock(ConfigurationRepository.class);
        when(configurationRepository.getConfiguration(CType.ROLES)).thenReturn(dlsRolesConfig);
        when(configurationRepository.getParserContext()).thenReturn(context);
        return configurationRepository;
    }

    private static ConfigurationRepository.Context getMockContextWithMatchAllQueryParser() {
        NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(List.of(
                new NamedXContentRegistry.Entry(QueryBuilder.class, new ParseField(MatchAllQueryBuilder.NAME), MatchAllQueryBuilder::fromXContent)
        ));
        return new ConfigurationRepository.Context(null, null, null, xContentRegistry, null);
    }

    @Test
    public void testUserAuthWithDLSRole() throws Exception {
        InternalAuthTokenProvider subject = new InternalAuthTokenProvider(null, privilegesEvaluator, actions, getMockConfigurationRepository(dlsRolesConfig, getMockContextWithMatchAllQueryParser()));
        subject.setSigningKey(SIGNING_KEY);

        InternalAuthTokenProvider.AuthFromInternalAuthToken result = subject.userAuthFromToken(JWT_WITH_DLS_ROLE, "test_audience");

        assertNotNull(result);
        assertEquals("test_user", result.getUser().getName());
        assertEquals(ImmutableSet.of("dls_role"), result.getUser().getSearchGuardRoles());
    }

    @Test
    public void testUserAuthWithDLSRoleMissingContext() throws Exception {
        InternalAuthTokenProvider subject = new InternalAuthTokenProvider(null, privilegesEvaluator, actions, getMockConfigurationRepository(dlsRolesConfig, null));
        subject.setSigningKey(SIGNING_KEY);

        assertThrows(PrivilegesEvaluationException.class, () -> subject.userAuthFromToken(JWT_WITH_DLS_ROLE, "test_audience"));
    }
}
