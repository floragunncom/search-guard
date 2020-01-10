/*
 * Copyright 2015-2018 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */

package com.floragunn.searchguard.cache;

import java.nio.file.Path;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.Settings;

import com.floragunn.searchguard.auth.AuthenticationBackend;
import com.floragunn.searchguard.auth.AuthorizationBackend;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;

public class DummyAuthenticationBackend implements AuthenticationBackend {

    private static volatile long authCount;
    private static volatile long existsCount;

    public DummyAuthenticationBackend(final Settings settings, final Path configPath) {
    }

    @Override
    public String getType() {
        return "dummy";
    }

    @Override
    public User authenticate(AuthCredentials credentials) throws ElasticsearchSecurityException {
        authCount++;
        return new User(credentials.getUsername());
    }

    @Override
    public boolean exists(User user) {
        existsCount++;
        return true;
    }

    public static long getAuthCount() {
        return authCount;
    }

    public static long getExistsCount() {
        return existsCount;
    }
    
    public static void reset() {
        authCount=0;
        existsCount=0;
    }
}
