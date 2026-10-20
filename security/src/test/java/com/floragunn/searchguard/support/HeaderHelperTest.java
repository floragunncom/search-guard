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

package com.floragunn.searchguard.support;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.user.AuthDomainInfo;
import com.floragunn.searchguard.user.User;

public class HeaderHelperTest {

    @Test
    public void mirrorUserContextIntoHeaders_mirrorsTransientsIntoHeaders() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        User user = new User("test_user", AuthDomainInfo.TLS_CERT);
        TransportAddress remoteAddress = new TransportAddress(new InetSocketAddress(InetAddress.getLoopbackAddress(), 12345));

        threadContext.putTransient(ConfigConstants.SG_USER, user);
        threadContext.putTransient(ConfigConstants.SG_ORIGIN, "REST");
        threadContext.putTransient(ConfigConstants.SG_REMOTE_ADDRESS, remoteAddress);

        HeaderHelper.mirrorUserContextIntoHeaders(threadContext);

        User userFromHeader = (User) Base64Helper.deserializeObject(threadContext.getHeader(ConfigConstants.SG_USER_HEADER));
        Assert.assertEquals(user.getName(), userFromHeader.getName());
        Assert.assertEquals("REST", threadContext.getHeader(ConfigConstants.SG_ORIGIN_HEADER));
        InetSocketAddress remoteAddressFromHeader = (InetSocketAddress) Base64Helper
                .deserializeObject(threadContext.getHeader(ConfigConstants.SG_REMOTE_ADDRESS_HEADER));
        Assert.assertEquals(remoteAddress.address(), remoteAddressFromHeader);
    }

    @Test
    public void mirrorUserContextIntoHeaders_doesNotOverwriteExistingHeadersAndToleratesMissingTransients() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        User user = new User("test_user", AuthDomainInfo.TLS_CERT);
        String existingUserHeader = Base64Helper.serializeObject(new User("other_user", AuthDomainInfo.TLS_CERT));

        threadContext.putHeader(ConfigConstants.SG_USER_HEADER, existingUserHeader);
        threadContext.putTransient(ConfigConstants.SG_USER, user);

        HeaderHelper.mirrorUserContextIntoHeaders(threadContext);

        Assert.assertEquals(existingUserHeader, threadContext.getHeader(ConfigConstants.SG_USER_HEADER));
        Assert.assertNull(threadContext.getHeader(ConfigConstants.SG_ORIGIN_HEADER));
        Assert.assertNull(threadContext.getHeader(ConfigConstants.SG_REMOTE_ADDRESS_HEADER));

        // No transients at all: must be a no-op
        ThreadContext emptyThreadContext = new ThreadContext(Settings.EMPTY);
        HeaderHelper.mirrorUserContextIntoHeaders(emptyThreadContext);
        Assert.assertNull(emptyThreadContext.getHeader(ConfigConstants.SG_USER_HEADER));
    }

    /**
     * Simulates what Elasticsearch does in the chunked fetch phase (TransportFetchPhaseCoordinationAction, since ES 9.5): The thread context
     * headers are captured, the context is stashed and only the captured headers are re-applied. The user must survive this.
     */
    @Test
    public void mirrorUserContextIntoHeaders_userSurvivesStashAndReapplyOfHeaders() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        User user = new User("test_user", AuthDomainInfo.TLS_CERT);
        threadContext.putTransient(ConfigConstants.SG_USER, user);
        threadContext.putTransient(ConfigConstants.SG_ORIGIN, "REST");

        HeaderHelper.mirrorUserContextIntoHeaders(threadContext);

        Map<String, String> capturedHeaders = new HashMap<>(threadContext.getHeaders());

        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            Assert.assertNull(threadContext.getTransient(ConfigConstants.SG_USER));

            for (Map.Entry<String, String> entry : capturedHeaders.entrySet()) {
                if (threadContext.getHeader(entry.getKey()) == null) {
                    threadContext.putHeader(entry.getKey(), entry.getValue());
                }
            }

            User userFromHeader = (User) Base64Helper.deserializeObject(threadContext.getHeader(ConfigConstants.SG_USER_HEADER));
            Assert.assertEquals(user.getName(), userFromHeader.getName());
            Assert.assertEquals("REST", threadContext.getHeader(ConfigConstants.SG_ORIGIN_HEADER));
        }
    }
}
