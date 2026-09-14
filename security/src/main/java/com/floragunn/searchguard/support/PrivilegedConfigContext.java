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

import java.util.List;
import java.util.Map;

import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;

import com.floragunn.searchguard.user.User;

/** Establishes the isolated thread context used for privileged Search Guard configuration requests. */
public final class PrivilegedConfigContext {

    private PrivilegedConfigContext() {
    }

    /**
     * Stashes the complete current thread context and initializes an isolated context for privileged
     * Search Guard configuration operations. The new context contains the configuration-request marker,
     * the canonical user, remote-address and origin headers, their transient caches, and the response
     * headers which were already present in the caller context. Other request headers and transients are
     * intentionally not copied.
     * <p>
     * The returned {@link StoredContext} owns restoration of the caller context and must be closed, normally
     * with try-with-resources. Until it is closed, the current thread is operating in the privileged context.
     * Callers dispatching asynchronous work must capture a {@link ThreadContext#newRestorableContext(boolean)}
     * before invoking this method and restore it in continuations which must run as the original caller. In
     * particular, user-facing listeners must not be invoked while the privileged context is active.
     *
     * @param threadContext the thread context containing the caller's canonical security headers
     * @return a stored context which restores the complete caller context when closed
     */
    public static StoredContext initPrivilegedContext(ThreadContext threadContext) {
        User user = SearchGuardContext.getUser(threadContext);
        TransportAddress remoteAddress = SearchGuardContext.getRemoteAddress(threadContext);
        String origin = SearchGuardContext.getOrigin(threadContext);
        Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();
        StoredContext storedContext = threadContext.stashContext();

        try {
            threadContext.putHeader(ConfigConstants.SG_CONF_REQUEST_HEADER, "true");
            SearchGuardContext.setUser(threadContext, user);
            SearchGuardContext.setRemoteAddress(threadContext, remoteAddress);
            SearchGuardContext.setOrigin(threadContext, origin);
            responseHeaders.forEach((name, values) -> values.forEach(value -> threadContext.addResponseHeader(name, value)));
            return storedContext;
        } catch (RuntimeException | Error e) {
            storedContext.close();
            throw e;
        }
    }
}
