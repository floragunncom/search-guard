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

package com.floragunn.signals.api;

import com.floragunn.searchguard.SignalsTenantParamResolver;
import com.floragunn.searchguard.authc.rest.TenantAwareRestHandler;
import com.floragunn.searchguard.authc.session.BaseRequestMetaData;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.action.StandardResponse;
import org.apache.http.HttpStatus;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;

/**
 * Base class for Signals' REST handlers which support different tenants
 */
public abstract class SignalsTenantAwareRestHandler extends SignalsBaseRestHandler implements TenantAwareRestHandler {

    protected SignalsTenantAwareRestHandler(Settings settings) {
        super(settings);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        BaseRequestMetaData requestMetaData = BaseRequestMetaData.adapt(request);
        if (User.USER_TENANT.equals(SignalsTenantParamResolver.getRequestedTenant(requestMetaData))) {
            return unsupportedPrivateTenantErrorResponse(request);
        }
        return getRestChannelConsumer(request, client);
    }

    protected abstract RestChannelConsumer getRestChannelConsumer(RestRequest request, NodeClient client) throws IOException;

    private RestChannelConsumer unsupportedPrivateTenantErrorResponse(RestRequest request) {
        //consume params in order to avoid `unrecognized parameters` error
        request.params().keySet().forEach(request::param);

        //consume content in order to avoid `request does not support having body` error
        request.content();

        return channel -> {

            StandardResponse standardResponse = new StandardResponse(HttpStatus.SC_BAD_REQUEST)
                    .error("Signals does not support private tenants");
            channel.sendResponse(standardResponse.toRestResponse());
        };
    }
}
