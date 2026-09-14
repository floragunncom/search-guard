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
 */

package com.floragunn.searchguard.support;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.junit.Test;

import com.floragunn.searchguard.user.User;

public class SearchGuardContextTest {

    @Test
    public void settersWriteCanonicalHeadersAndTransientCaches() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        User user = new User("test_user");
        TransportAddress remoteAddress = new TransportAddress(new InetSocketAddress(InetAddress.getLoopbackAddress(), 1234));

        SearchGuardContext.setUser(threadContext, user);
        SearchGuardContext.setRemoteAddress(threadContext, remoteAddress);
        SearchGuardContext.setOrigin(threadContext, "REST");

        assertEquals(user, Base64Helper.deserializeObject(threadContext.getHeader(ConfigConstants.SG_USER_HEADER)));
        assertEquals(remoteAddress.address(),
                Base64Helper.deserializeObject(threadContext.getHeader(ConfigConstants.SG_REMOTE_ADDRESS_HEADER)));
        assertEquals("REST", threadContext.getHeader(ConfigConstants.SG_ORIGIN_HEADER));
        assertSame(user, threadContext.getTransient(ConfigConstants.SG_USER));
        assertSame(remoteAddress, threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS));
        assertEquals("REST", threadContext.getTransient(ConfigConstants.SG_ORIGIN));
    }

    @Test
    public void gettersRecreateTransientCachesFromCanonicalHeaders() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        User user = new User("test_user");
        TransportAddress remoteAddress = new TransportAddress(new InetSocketAddress(InetAddress.getLoopbackAddress(), 1234));

        threadContext.putHeader(Map.of(
                ConfigConstants.SG_USER_HEADER, Base64Helper.serializeObject(user),
                ConfigConstants.SG_REMOTE_ADDRESS_HEADER, Base64Helper.serializeObject(remoteAddress.address()),
                ConfigConstants.SG_ORIGIN_HEADER, "REST"));

        assertNull(threadContext.getTransient(ConfigConstants.SG_USER));
        assertNull(threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS));
        assertNull(threadContext.getTransient(ConfigConstants.SG_ORIGIN));

        assertEquals(user, SearchGuardContext.getUser(threadContext));
        assertEquals(remoteAddress, SearchGuardContext.getRemoteAddress(threadContext));
        assertEquals("REST", SearchGuardContext.getOrigin(threadContext));
    }

    @Test
    public void privilegedConfigContextIsIsolatedAndRestoresItsCaller() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        User user = new User("test_user");
        TransportAddress remoteAddress = new TransportAddress(new InetSocketAddress(InetAddress.getLoopbackAddress(), 1234));
        SearchGuardContext.setUser(threadContext, user);
        SearchGuardContext.setRemoteAddress(threadContext, remoteAddress);
        SearchGuardContext.setOrigin(threadContext, "REST");
        threadContext.putHeader("unrelated", "value");
        threadContext.addResponseHeader("warning", "original warning");

        try (StoredContext ignored = PrivilegedConfigContext.initPrivilegedContext(threadContext)) {
            assertEquals("true", threadContext.getHeader(ConfigConstants.SG_CONF_REQUEST_HEADER));
            assertEquals(user, SearchGuardContext.getUser(threadContext));
            assertEquals(remoteAddress, SearchGuardContext.getRemoteAddress(threadContext));
            assertEquals("REST", SearchGuardContext.getOrigin(threadContext));
            assertNull(threadContext.getHeader("unrelated"));
            assertEquals(Map.of("warning", java.util.List.of("original warning")), threadContext.getResponseHeaders());
        }

        assertNull(threadContext.getHeader(ConfigConstants.SG_CONF_REQUEST_HEADER));
        assertEquals("value", threadContext.getHeader("unrelated"));
        assertEquals(user, SearchGuardContext.getUser(threadContext));
    }
}
