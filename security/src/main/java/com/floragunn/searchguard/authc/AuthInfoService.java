/*
 * Copyright 2021-2022 floragunn GmbH
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

package com.floragunn.searchguard.authc;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContext;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContextProviderRegistry;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;

public class AuthInfoService {
    private final ThreadPool threadPool;
    private final SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry;
    private final AdminDNs adminDNs;

    public AuthInfoService(ThreadPool threadPool,
            SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry, AdminDNs adminDNs) {
        this.threadPool = threadPool;
        this.specialPrivilegesEvaluationContextProviderRegistry = specialPrivilegesEvaluationContextProviderRegistry;
        this.adminDNs = adminDNs;
    }

    public User getCurrentUser() {
        User user = peekCurrentUser();

        if (user == null) {
            throw new ElasticsearchSecurityException("No user information available");
        }

        return user;
    }

    public User peekCurrentUser() {
        return threadPool.getThreadContext().getTransient(ConfigConstants.SG_USER);
    }

    public boolean isCurrentUserAdmin() {
        return this.isAdmin(this.peekCurrentUser());
    }

    public boolean isAdmin(User user) {
        if (user != null) {
            return this.adminDNs.isAdmin(user);
        } else {
            return false;
        }
    }

    public TransportAddress getCurrentRemoteAddress() {
        return (TransportAddress) this.threadPool.getThreadContext().getTransient(ConfigConstants.SG_REMOTE_ADDRESS);
    }

    public SpecialPrivilegesEvaluationContext getSpecialPrivilegesEvaluationContext() {
        return specialPrivilegesEvaluationContextProviderRegistry.provide(getCurrentUser(), threadPool.getThreadContext());
    }

}
