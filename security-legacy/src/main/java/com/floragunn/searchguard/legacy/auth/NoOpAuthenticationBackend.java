/*
 * Copyright 2015-2017 floragunn GmbH
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

package com.floragunn.searchguard.legacy.auth;

import java.nio.file.Path;

import org.elasticsearch.common.settings.Settings;

import com.floragunn.searchguard.TypedComponent;
import com.floragunn.searchguard.TypedComponent.Factory;
import com.floragunn.searchguard.authc.AuthenticationBackend.UserCachingPolicy;
import com.floragunn.searchguard.authc.legacy.LegacyAuthenticationBackend;
import com.floragunn.searchguard.legacy.LegacyComponentFactory;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;

public class NoOpAuthenticationBackend implements LegacyAuthenticationBackend {

    public NoOpAuthenticationBackend(final Settings settings, final Path configPath) {
        super();
    }

    @Override
    public String getType() {
        return "noop";
    }

    @Override
    public User authenticate(final AuthCredentials credentials) {
        return User.forUser(credentials.getUsername()).with(credentials).build();
    }

    @Override
    public boolean exists(User user) {
        return true;
    }

    @Override
    public UserCachingPolicy userCachingPolicy() {
        return UserCachingPolicy.ONLY_IF_AUTHZ_SEPARATE;
    }

    public static TypedComponent.Info<LegacyAuthenticationBackend> INFO = new TypedComponent.Info<LegacyAuthenticationBackend>() {

        @Override
        public Class<LegacyAuthenticationBackend> getType() {
            return LegacyAuthenticationBackend.class;
        }

        @Override
        public String getName() {
            return "noop";
        }

        @Override
        public Factory<LegacyAuthenticationBackend> getFactory() {
            return LegacyComponentFactory.adapt(NoOpAuthenticationBackend::new);
        }
    };
}
