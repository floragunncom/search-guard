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

import org.opensearch.OpenSearchSecurityException;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;

import com.floragunn.searchguard.auth.HTTPAuthenticator;
import com.floragunn.searchguard.user.AuthCredentials;

public class DummyHTTPAuthenticator implements HTTPAuthenticator {

    private static volatile long count;

    public DummyHTTPAuthenticator(final Settings settings, final Path configPath) {
    }

    @Override
    public String getType() {
        return "dummy";
    }

    @Override
    public AuthCredentials extractCredentials(RestRequest request, ThreadContext context) throws OpenSearchSecurityException {
        count++;
        return AuthCredentials.forUser("dummy").complete().build();
    }

    @Override
    public boolean reRequestAuthentication(RestChannel channel, AuthCredentials credentials) {
        return false;
    }

    public static long getCount() {
        return count;
    }
    
    public static void reset() {
        count=0;
    }
}
