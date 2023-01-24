/*
 * Copyright 2023 floragunn GmbH
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
package com.floragunn.signals.watch.common;

import com.floragunn.searchguard.support.WildcardMatcher;
import java.net.URI;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HttpEndpointWhitelist {
    private final static Logger log = LogManager.getLogger(HttpEndpointWhitelist.class);

    private final List<String> whitelist;

    public HttpEndpointWhitelist(List<String> whitelist) {
        this.whitelist = whitelist;
    }

    public void check(URI uri) throws NotWhitelistedException {
        String uriAsString = uri.toString();

        if (log.isDebugEnabled()) {
            log.debug("Checking " + uri + " against " + whitelist);
        }

        for (String entry : whitelist) {
            if (WildcardMatcher.match(entry, uriAsString)) {
                return;
            }
        }

        throw new NotWhitelistedException(uri);
    }

    public static class NotWhitelistedException extends Exception {

        private static final long serialVersionUID = 5274286136737656655L;

        public NotWhitelistedException(URI uri) {
            super("The URI is not whitelisted: " + uri);
        }
    }
}
