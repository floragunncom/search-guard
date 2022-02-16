/*
 * Copyright 2022 floragunn GmbH
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

package com.floragunn.searchguard.authc.rest.authenticators;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.AuthCredentials;

public class HTTPTrustedOriginAuthenticator implements HTTPAuthenticator {

    protected final Logger log = LogManager.getLogger(this.getClass());

    public HTTPTrustedOriginAuthenticator(DocNode docNode, ConfigurationRepository.Context context) {

    }

    public AuthCredentials extractCredentials(RestRequest restRequest, ThreadContext threadContext) {
        if (threadContext.getTransient(ConfigConstants.SG_XFF_DONE) == Boolean.TRUE) {
            return AuthCredentials.forUser("n/a").authenticatorType(getType()).complete().build();
        } else {
            return null;
        }
    }

    @Override
    public String getType() {
        return "trusted_origin";
    }
}