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

package com.floragunn.searchguard.action.whoami;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.internal.ClusterAdminClient;
import org.elasticsearch.client.internal.ElasticsearchClient;


public class WhoAmIRequestBuilder extends
ActionRequestBuilder<WhoAmIRequest, WhoAmIResponse> {    
    public WhoAmIRequestBuilder(final ClusterAdminClient client) {
        this(client, WhoAmIAction.INSTANCE);
    }

    public WhoAmIRequestBuilder(final ElasticsearchClient client, final WhoAmIAction action) {
        super(client, action, new WhoAmIRequest());
    }
}
