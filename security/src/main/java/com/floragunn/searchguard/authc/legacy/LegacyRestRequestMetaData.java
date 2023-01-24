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
package com.floragunn.searchguard.authc.legacy;

import com.floragunn.searchguard.authc.rest.ClientAddressAscertainer.ClientIpInfo;
import com.floragunn.searchguard.authc.rest.RestRequestMetaData;
import inet.ipaddr.IPAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;

public class LegacyRestRequestMetaData extends RestRequestMetaData {
    private final ThreadContext threadContext;

    public LegacyRestRequestMetaData(RestRequest request, ClientIpInfo clientIpInfo, String clientCertSubject, ThreadContext threadContext) {
        super(request, clientIpInfo, clientCertSubject);
        this.threadContext = threadContext;
    }

    public LegacyRestRequestMetaData(RestRequest request, IPAddress ipAddress, String clientCertSubject, ThreadContext threadContext) {
        super(request, ipAddress, clientCertSubject);
        this.threadContext = threadContext;
    }

    public ThreadContext getThreadContext() {
        return threadContext;
    }
}
