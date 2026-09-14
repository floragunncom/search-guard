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

import java.net.InetSocketAddress;
import java.util.Objects;

import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import com.floragunn.searchguard.user.User;

/**
 * Access to the security identity stored in a {@link ThreadContext}.
 *
 * The serializable request headers are the canonical representation because they survive context
 * propagation and transport serialization. The corresponding transient values are local caches for
 * consumers which need the deserialized objects.
 */
public final class SearchGuardContext {

    private SearchGuardContext() {
    }

    public static User getUser(ThreadContext threadContext) {
        String userHeader = threadContext.getHeader(ConfigConstants.SG_USER_HEADER);
        User cachedUser = threadContext.getTransient(ConfigConstants.SG_USER);

        if (userHeader == null) {
            if (cachedUser != null) {
                threadContext.putHeader(ConfigConstants.SG_USER_HEADER, Base64Helper.serializeObject(cachedUser));
            }
            return cachedUser;
        }

        if (cachedUser == null) {
            cachedUser = Objects.requireNonNull((User) Base64Helper.deserializeObject(userHeader));
            threadContext.putTransient(ConfigConstants.SG_USER, cachedUser);
        }

        return cachedUser;
    }

    public static void setUser(ThreadContext threadContext, User user) {
        if (user == null) {
            return;
        }

        putCanonicalHeader(threadContext, ConfigConstants.SG_USER_HEADER, Base64Helper.serializeObject(user));

        if (threadContext.getTransient(ConfigConstants.SG_USER) == null) {
            threadContext.putTransient(ConfigConstants.SG_USER, user);
        }
    }

    public static TransportAddress getRemoteAddress(ThreadContext threadContext) {
        String remoteAddressHeader = threadContext.getHeader(ConfigConstants.SG_REMOTE_ADDRESS_HEADER);
        TransportAddress cachedRemoteAddress = threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS);

        if (remoteAddressHeader == null) {
            if (cachedRemoteAddress != null) {
                threadContext.putHeader(ConfigConstants.SG_REMOTE_ADDRESS_HEADER,
                        Base64Helper.serializeObject(cachedRemoteAddress.address()));
            }
            return cachedRemoteAddress;
        }

        if (cachedRemoteAddress == null) {
            cachedRemoteAddress = new TransportAddress(
                    Objects.requireNonNull((InetSocketAddress) Base64Helper.deserializeObject(remoteAddressHeader)));
            threadContext.putTransient(ConfigConstants.SG_REMOTE_ADDRESS, cachedRemoteAddress);
        }

        return cachedRemoteAddress;
    }

    public static void setRemoteAddress(ThreadContext threadContext, TransportAddress remoteAddress) {
        if (remoteAddress == null) {
            return;
        }

        putCanonicalHeader(threadContext, ConfigConstants.SG_REMOTE_ADDRESS_HEADER,
                Base64Helper.serializeObject(remoteAddress.address()));

        if (threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS) == null) {
            threadContext.putTransient(ConfigConstants.SG_REMOTE_ADDRESS, remoteAddress);
        }
    }

    public static String getOrigin(ThreadContext threadContext) {
        String originHeader = threadContext.getHeader(ConfigConstants.SG_ORIGIN_HEADER);
        String cachedOrigin = threadContext.getTransient(ConfigConstants.SG_ORIGIN);

        if (originHeader == null) {
            if (cachedOrigin != null) {
                threadContext.putHeader(ConfigConstants.SG_ORIGIN_HEADER, cachedOrigin);
            }
            return cachedOrigin;
        }

        if (cachedOrigin == null) {
            threadContext.putTransient(ConfigConstants.SG_ORIGIN, originHeader);
            return originHeader;
        }

        return cachedOrigin;
    }

    public static void setOrigin(ThreadContext threadContext, String origin) {
        if (origin == null) {
            return;
        }

        putCanonicalHeader(threadContext, ConfigConstants.SG_ORIGIN_HEADER, origin);

        if (threadContext.getTransient(ConfigConstants.SG_ORIGIN) == null) {
            threadContext.putTransient(ConfigConstants.SG_ORIGIN, origin);
        }
    }

    public static void initializeTransientCaches(ThreadContext threadContext) {
        getUser(threadContext);
        getRemoteAddress(threadContext);
        getOrigin(threadContext);
    }

    private static void putCanonicalHeader(ThreadContext threadContext, String name, String value) {
        String existingValue = threadContext.getHeader(name);

        if (existingValue == null) {
            threadContext.putHeader(name, value);
        } else if (!existingValue.equals(value)) {
            throw new IllegalStateException("Cannot replace canonical Search Guard header [" + name + "]");
        }
    }
}
