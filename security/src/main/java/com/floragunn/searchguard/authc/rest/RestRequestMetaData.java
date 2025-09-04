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

package com.floragunn.searchguard.authc.rest;

import java.util.List;
import java.util.Map;

import org.elasticsearch.rest.RestRequest;

import com.floragunn.searchguard.authc.RequestMetaData;
import com.floragunn.searchguard.authc.rest.ClientAddressAscertainer.ClientIpInfo;

import inet.ipaddr.IPAddress;

public class RestRequestMetaData extends RequestMetaData<RestRequest> {

    public RestRequestMetaData(RestRequest request, ClientIpInfo clientIpInfo, String clientCertSubject) {
        super(request, clientIpInfo, clientCertSubject);
    }

    public RestRequestMetaData(RestRequest request, IPAddress ipAddress, String clientCertSubject) {
        super(request, ipAddress, clientCertSubject);
    }

    @Override
    public String getUri() {
        return getRequest().uri();
    }

    @Override
    public String getHeader(String headerName) {
        return getRequest().header(headerName);
    }

    @Override
    public Map<String, List<String>> getHeaders() {
        return getRequest().getHeaders();
    }

    @Override
    public String getParam(String paramName) {
        return getRequest().param(paramName);
    }

}
