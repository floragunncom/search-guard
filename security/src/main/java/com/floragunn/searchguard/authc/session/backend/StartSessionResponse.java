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

package com.floragunn.searchguard.authc.session.backend;

import java.io.IOException;

import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

public class StartSessionResponse implements ToXContentObject {
    private String token;
    private String redirectUri;
    
    public StartSessionResponse(String token, String redirectUri) {
        this.token = token;
        this.redirectUri = redirectUri;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("token", token);
        builder.field("redirect_uri", redirectUri);        
        builder.endObject();
        return builder;
    }

    public String getToken() {
        return token;
    }
    
    public String getRedirectUri() {
        return redirectUri;
    }
}
