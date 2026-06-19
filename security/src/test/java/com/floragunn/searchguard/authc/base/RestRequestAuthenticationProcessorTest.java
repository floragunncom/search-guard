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

package com.floragunn.searchguard.authc.base;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.elasticsearch.rest.RestRequest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.authc.AuthenticationDomain;
import com.floragunn.searchguard.authc.RequestMetaData;
import com.floragunn.searchguard.authc.blocking.BlockedUserRegistry;
import com.floragunn.searchguard.authc.rest.HttpAuthenticationFrontend;
import com.floragunn.searchguard.authc.rest.RestRequestAuthenticationProcessor;
import com.floragunn.searchguard.authz.PrivilegesEvaluator;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;
import com.google.common.cache.Cache;

@RunWith(MockitoJUnitRunner.class)
public class RestRequestAuthenticationProcessorTest {

    @Mock private RequestMetaData<RestRequest> request;
    @Mock private AuthenticationDomain<HttpAuthenticationFrontend> authenticationDomain;
    @Mock private HttpAuthenticationFrontend authenticationFrontend;
    @Mock private AdminDNs adminDns;
    @Mock private PrivilegesEvaluator privilegesEvaluator;
    @SuppressWarnings("unchecked") @Mock private Cache<AuthCredentials, User> userCache;
    @SuppressWarnings("unchecked") @Mock private Cache<String, User> impersonationCache;
    @Mock private AuditLog auditLog;
    @Mock private BlockedUserRegistry blockedUserRegistry;

    /**
     * Verifies that header values taken from the Netty request (which may be backed by a non-serializable
     * internal Netty list type) are replaced with serializable {@link List#copyOf} instances before being
     * stored as user-mapping attributes on the credentials that are passed down the auth pipeline.
     *
     * <p>Without the normalization, cross-node transport throws {@code NotSerializableException}
     * when trying to Java-serialize the {@link User} context.
     */
    @Test
    public void requestHeaderValuesAreNormalizedToSerializableTypes() throws Exception {
        // Simulate Netty's non-serializable list (e.g. io.netty.handler.codec.HeadersUtils$1)
        List<String> nonSerializableList = new AbstractList<String>() {
            @Override public String get(int index) { return "attr-value"; }
            @Override public int size() { return 1; }
        };
        Assert.assertFalse("pre-condition: test list must not be Serializable",
                nonSerializableList instanceof Serializable);

        Mockito.when(authenticationDomain.getFrontend()).thenReturn(authenticationFrontend);
        Mockito.when(authenticationFrontend.extractCredentials(request))
                .thenReturn(AuthCredentials.forUser("test-user").complete().build());
        Mockito.when(request.getHeaders())
                .thenReturn(Map.of("x-proxy-attr-accesslog", nonSerializableList));

        List<AuthCredentials> capturedCredentials = new ArrayList<>();

        // Declared as RequestAuthenticationProcessor so that handleCurrentAuthenticationDomain
        // is accessible (it is protected in the same package as this test).
        RequestAuthenticationProcessor<HttpAuthenticationFrontend> processor =
                new RestRequestAuthenticationProcessor(
                        request, List.of(authenticationDomain), adminDns, privilegesEvaluator,
                        userCache, impersonationCache, auditLog, blockedUserRegistry,
                        List.of(), List.of(), false) {
                    @Override
                    protected AuthDomainState proceed(AuthCredentials ac,
                            AuthenticationDomain<HttpAuthenticationFrontend> authDomain,
                            Consumer<AuthcResult> onResult, Consumer<Exception> onFailure) {
                        capturedCredentials.add(ac);
                        return AuthDomainState.SKIP;
                    }
                };

        processor.handleCurrentAuthenticationDomain(authenticationDomain, result -> {}, error -> {});

        Assert.assertFalse("proceed() must have been called with enriched credentials", capturedCredentials.isEmpty());

        @SuppressWarnings("unchecked")
        Map<String, Object> requestAttrs = (Map<String, Object>) capturedCredentials.get(0)
                .getAttributesForUserMapping().get("request");
        @SuppressWarnings("unchecked")
        Map<String, Object> headers = (Map<String, Object>) requestAttrs.get("headers");

        Object headerValue = headers.get("x-proxy-attr-accesslog");
        Assert.assertNotNull("header value must be present in user mapping attributes", headerValue);
        Assert.assertTrue("header list value must be Serializable", headerValue instanceof Serializable);
        Assert.assertNotSame("header list value must be a defensive copy, not the original Netty-backed list",
                nonSerializableList, headerValue);

        // Serialize the header value as it would be stored in User.structuredAttributes after UserMapping
        // extracts it via JSONPath. That path — not the ImmutableMap container — is what Base64Helper
        // Java-serializes for inter-node transport.
        byte[] serialized;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(headerValue);
            serialized = baos.toByteArray();
        }

        Object deserializedHeaderValue;
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(serialized))) {
            deserializedHeaderValue = ois.readObject();
        }

        Assert.assertEquals(headerValue, deserializedHeaderValue);
    }
}