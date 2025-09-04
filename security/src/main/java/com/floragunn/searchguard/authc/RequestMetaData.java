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

package com.floragunn.searchguard.authc;

import java.util.List;
import java.util.Map;

import com.floragunn.searchguard.authc.session.BaseRequestMetaData;
import org.elasticsearch.rest.RestRequest;

import com.floragunn.searchguard.authc.rest.ClientAddressAscertainer.ClientIpInfo;

import inet.ipaddr.IPAddress;

public abstract class RequestMetaData<T> implements BaseRequestMetaData {
    private final T request;
    private final IPAddress directIpAddress;
    private final IPAddress originatingIpAddress;
    private final boolean trustedProxy;
    private final String clientCertSubject;

    public RequestMetaData(T request, ClientIpInfo clientIpInfo, String clientCertSubject) {
        this.request = request;
        this.directIpAddress = clientIpInfo.getDirectIpAddress();
        this.originatingIpAddress = clientIpInfo.getOriginatingIpAddress();
        this.trustedProxy = clientIpInfo.isTrustedProxy();
        this.clientCertSubject = clientCertSubject;
    }

    public RequestMetaData(T request, IPAddress ipAddress, String clientCertSubject) {
        this.request = request;
        this.directIpAddress = ipAddress;
        this.originatingIpAddress = ipAddress;
        this.clientCertSubject = clientCertSubject;
        this.trustedProxy = false;
    }

    public T getRequest() {
        return request;
    }

    public IPAddress getDirectIpAddress() {
        return directIpAddress;
    }

    public IPAddress getOriginatingIpAddress() {
        return originatingIpAddress;
    }

    public boolean isTrustedProxy() {
        return trustedProxy;
    }

    public String getClientCertSubject() {
        return clientCertSubject;
    }

    public abstract String getHeader(String headerName);

    public abstract Map<String, List<String>> getHeaders();

    public abstract String getParam(String paramName);
    
    public String getAuthorizationByScheme(String header, String scheme) {
        String authorization = getHeader(header);
        if (authorization == null) {
            return null;
        }

        int separator = authorization.indexOf(' ');
        if (separator == -1) {
            return null;
        }

        if (!authorization.substring(0, separator).equalsIgnoreCase(scheme)) {
            return null;
        }

        return authorization.substring(separator + 1).trim();
    }

    public String getAuthorizationByScheme(String scheme) {
        return getAuthorizationByScheme("Authorization", scheme);
    }

    @Override
    public String toString() {
        if (request instanceof RestRequest) {
            return "[request=" + ((RestRequest) request).path() + ", directIpAddress=" + getDirectIpAddress() + ", originatingIpAddress="
                    + getOriginatingIpAddress() + ", clientCertSubject=" + clientCertSubject + "]";
        } else {
            return "[request=" + request + ", directIpAddress=" + getDirectIpAddress() + ", originatingIpAddress=" + getOriginatingIpAddress()
                    + ", clientCertSubject=" + clientCertSubject + "]";
        }
    }
}
