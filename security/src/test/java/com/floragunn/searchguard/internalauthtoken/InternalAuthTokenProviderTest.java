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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Base64;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.Test;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.actions.Actions;
import com.floragunn.searchguard.authz.config.ActionGroup;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.internalauthtoken.InternalAuthTokenProvider.AuthFromInternalAuthToken;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.user.User;
import com.nimbusds.jose.crypto.MACVerifier;
import com.nimbusds.jwt.SignedJWT;


public class InternalAuthTokenProviderTest {

    static final Actions actions = new Actions(null);
    static final SgDynamicConfiguration<Role> rolesConfig = TestSgConfig.Role.toActualRole(//                    
            new TestSgConfig.Role("role_1").indexPermissions("*").on("index_a*"),
            new TestSgConfig.Role("role_2").indexPermissions("*").on("index_b*"),
            new TestSgConfig.Role("role_3").indexPermissions("*").on("index_c*"));
    static final Function<User, ImmutableSet<String>> roleMapper = (user) -> ImmutableSet.of("role_1", "role_2");
    static final Supplier<ActionGroup.FlattenedIndex> emptyActionGroupSupplier = () -> ActionGroup.FlattenedIndex.EMPTY;
    static final Supplier<Set<String>> emptyTenantNameSupplier = ImmutableSet::empty;
    
    static final String SIGNING_KEY = "8me7E2GunuIMaeBCbwl+7Le4TzydK7Sv2/kr0p4EVcqisyT3U5qkExBYVMAycYfYyN3Q/e8YYrWd2kZKWVkCJg==";
            
    static final String JWT_PRODUCED_BY_CXF = "eyJhbGciOiJIUzUxMiJ9.eyJuYmYiOjE3NDE5NDcwMjIsInN1YiI6InRlc3RfdXNlciIsImF1ZCI6InRlc3RfYXVkaWVuY2UiLCJzZ19yb2xlcyI6eyJyb2xlXzEiOnsiY2x1c3Rlcl9wZXJtaXNzaW9ucyI6W10sImluZGV4X3Blcm1pc3Npb25zIjpbeyJpbmRleF9wYXR0ZXJucyI6WyJpbmRleF9hKiJdLCJhbGxvd2VkX2FjdGlvbnMiOlsiKiJdfV0sImFsaWFzX3Blcm1pc3Npb25zIjpbXSwiZGF0YV9zdHJlYW1fcGVybWlzc2lvbnMiOltdLCJ0ZW5hbnRfcGVybWlzc2lvbnMiOltdLCJleGNsdWRlX2NsdXN0ZXJfcGVybWlzc2lvbnMiOltdfSwicm9sZV8yIjp7ImNsdXN0ZXJfcGVybWlzc2lvbnMiOltdLCJpbmRleF9wZXJtaXNzaW9ucyI6W3siaW5kZXhfcGF0dGVybnMiOlsiaW5kZXhfYioiXSwiYWxsb3dlZF9hY3Rpb25zIjpbIioiXX1dLCJhbGlhc19wZXJtaXNzaW9ucyI6W10sImRhdGFfc3RyZWFtX3Blcm1pc3Npb25zIjpbXSwidGVuYW50X3Blcm1pc3Npb25zIjpbXSwiZXhjbHVkZV9jbHVzdGVyX3Blcm1pc3Npb25zIjpbXX19fQ.dsvhX9hKAmxgWljcxBTHjM_T8o53IuGlF1hs-IMTO5iYPgr543ZSwAjtMdcrEmvtm3aJ49U3NWoV7LHVqZrkTw";

    static final String JWT_PRODUCED_BY_NIMBUS = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ0ZXN0X3VzZXIiLCJhdWQiOiJ0ZXN0X2F1ZGllbmNlIiwibmJmIjoxNzQxOTQ2LCJzZ19pIjoibiIsInNnX3JvbGVzIjp7InJvbGVfMSI6eyJjbHVzdGVyX3Blcm1pc3Npb25zIjpbXSwiaW5kZXhfcGVybWlzc2lvbnMiOlt7ImluZGV4X3BhdHRlcm5zIjpbImluZGV4X2EqIl0sImFsbG93ZWRfYWN0aW9ucyI6WyIqIl19XSwiYWxpYXNfcGVybWlzc2lvbnMiOltdLCJkYXRhX3N0cmVhbV9wZXJtaXNzaW9ucyI6W10sInRlbmFudF9wZXJtaXNzaW9ucyI6W10sImV4Y2x1ZGVfY2x1c3Rlcl9wZXJtaXNzaW9ucyI6W119LCJyb2xlXzIiOnsiY2x1c3Rlcl9wZXJtaXNzaW9ucyI6W10sImluZGV4X3Blcm1pc3Npb25zIjpbeyJpbmRleF9wYXR0ZXJucyI6WyJpbmRleF9iKiJdLCJhbGxvd2VkX2FjdGlvbnMiOlsiKiJdfV0sImFsaWFzX3Blcm1pc3Npb25zIjpbXSwiZGF0YV9zdHJlYW1fcGVybWlzc2lvbnMiOltdLCJ0ZW5hbnRfcGVybWlzc2lvbnMiOltdLCJleGNsdWRlX2NsdXN0ZXJfcGVybWlzc2lvbnMiOltdfX19.gcWLI-BOMQH9M70bQSaltGjHpbR7XPqzCrcJd4iS4A0PPsGSJLohQ4MqiWnCjd7ytB__MIek3GkbuhTJZxquyQ";
    
    @Test
    public void getJwt() throws Exception {
        InternalAuthTokenProvider subject = new InternalAuthTokenProvider(roleMapper, emptyActionGroupSupplier, emptyTenantNameSupplier, actions,
                () -> rolesConfig);        
        subject.setSigningKey(SIGNING_KEY);

       String jwt = subject.getJwt(new User("test_user"), "test_audience");
              
       SignedJWT signedJWT = SignedJWT.parse(jwt);
       assertTrue("JWT can be verified: " + jwt, signedJWT.verify(new MACVerifier(Base64.getDecoder().decode(SIGNING_KEY))));
    }
    
    @Test
    public void userAuthFromToken_jwtFromNimbus() throws Exception {
        InternalAuthTokenProvider subject = new InternalAuthTokenProvider(roleMapper, emptyActionGroupSupplier, emptyTenantNameSupplier, actions,
                () -> rolesConfig);        
        subject.setSigningKey(SIGNING_KEY);
        
        AuthFromInternalAuthToken result = subject.userAuthFromToken(JWT_PRODUCED_BY_NIMBUS, "test_audience");
        
        assertEquals("test_user", result.getUser().getName());
        assertEquals(ImmutableSet.of("role_1", "role_2"), result.getUser().getSearchGuardRoles());
    }
    
    @Test
    public void userAuthFromToken_jwtFromCxf() throws Exception {
        InternalAuthTokenProvider subject = new InternalAuthTokenProvider(roleMapper, emptyActionGroupSupplier, emptyTenantNameSupplier, actions,
                () -> rolesConfig);        
        subject.setSigningKey(SIGNING_KEY);
        
        AuthFromInternalAuthToken result = subject.userAuthFromToken(JWT_PRODUCED_BY_CXF, "test_audience");
        
        assertEquals("test_user", result.getUser().getName());
        assertEquals(ImmutableSet.of("role_1", "role_2"), result.getUser().getSearchGuardRoles());
    }

}
