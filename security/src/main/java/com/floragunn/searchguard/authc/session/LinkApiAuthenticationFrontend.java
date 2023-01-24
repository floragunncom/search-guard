/*
 * Copyright 2021 floragunn GmbH
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
package com.floragunn.searchguard.authc.session;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.searchguard.authc.session.ActivatedFrontendConfig.AuthMethod;
import com.floragunn.searchguard.authc.session.GetActivatedFrontendConfigAction.Request;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchsupport.cstate.ComponentState;
import java.net.URI;
import java.util.Map;
import org.elasticsearch.ElasticsearchSecurityException;

public class LinkApiAuthenticationFrontend implements ApiAuthenticationFrontend {

    private final ComponentState componentState = new ComponentState(0, "authentication_frontend", "link").initialized();
    private final URI url;

    public LinkApiAuthenticationFrontend(DocNode config, ConfigurationRepository.Context context) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(config, validationErrors);

        url = vNode.get("url").required().asAbsoluteURI();

        vNode.checkForUnusedAttributes();
        validationErrors.throwExceptionForPresentErrors();
    }

    @Override
    public String getType() {
        return "link";
    }

    @Override
    public AuthCredentials extractCredentials(Map<String, Object> request) throws ElasticsearchSecurityException, ConfigValidationException {
        // This authentication frontend cannot authenticate anyone. It is just for providing a link in the login menu.
        return null;
    }

    @Override
    public AuthMethod activateFrontendConfig(AuthMethod frontendConfig, Request request) {
        return frontendConfig.ssoLocation(url.toASCIIString());
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

}
