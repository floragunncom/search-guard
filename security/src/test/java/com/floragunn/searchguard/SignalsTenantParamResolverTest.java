/*
 * Copyright 2026 floragunn GmbH
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

package com.floragunn.searchguard;

import com.floragunn.searchsupport.util.EsLogging;
import org.elasticsearch.rest.RestRequest;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SignalsTenantParamResolverTest {

    @ClassRule
    public static EsLogging esLogging = new EsLogging();

    @Mock
    private RestRequest restRequest;


    @Test
    public void getRequestedTenant_shouldReturnTenantFromUri_urlEncodedTenant() {
        //space character in the tenant name
        when(restRequest.uri()).thenReturn("/_signals/watch/admin%20tenant/action");

        String tenant = SignalsTenantParamResolver.getRequestedTenant(restRequest);
        assertThat(tenant, equalTo("admin tenant"));
    }
}
