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

package com.floragunn.searchguard.authc.transport;

import java.util.List;
import java.util.Map;

import org.opensearch.transport.TransportRequest;

import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.authc.RequestMetaData;
import com.floragunn.searchguard.authc.rest.ClientAddressAscertainer.ClientIpInfo;

import inet.ipaddr.IPAddress;

public class TransportRequestMetaData extends RequestMetaData<TransportRequest> {

    TransportRequestMetaData(TransportRequest request, ClientIpInfo clientIpInfo, String clientCertSubject) {
        super(request, clientIpInfo, clientCertSubject);
    }

    TransportRequestMetaData(TransportRequest request, IPAddress ipAddress, String clientCertSubject) {
        super(request, ipAddress, clientCertSubject);
    }

    @Override
    public String getHeader(String headerName) {
        return null;
    }

    @Override
    public Map<String, List<String>> getHeaders() {
        return ImmutableMap.empty();
    }

    @Override
    public String getParam(String paramName) {
        return null;
    }

}
