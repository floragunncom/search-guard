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

import java.net.InetSocketAddress;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import com.floragunn.searchguard.support.IPAddressCollection;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import inet.ipaddr.AddressStringException;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressNetwork.IPAddressGenerator;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.IncompatibleAddressException;

public abstract class ClientAddressAscertainer {

    public static ClientAddressAscertainer create(String remoteIpHeader, IPAddressCollection trustedProxies) {
        if (trustedProxies != null) {
            return new CIDRBased(remoteIpHeader, trustedProxies);
        } else {
            return new Inactive();
        }
    }

    @Deprecated
    public static ClientAddressAscertainer create(String remoteIpHeader, Pattern trustedProxyPattern) {
        if (trustedProxyPattern != null) {
            return new PatternBased(remoteIpHeader, trustedProxyPattern);
        } else {
            return new Inactive();
        }
    }

    @SuppressWarnings("deprecation")
    public static ClientAddressAscertainer create(RestAuthcConfig.Network network) {
        if (network == null) {
            return new Inactive();
        } else if (network.getTrustedProxies() != null) {
            return create(network.getRemoteIpHttpHeader(), network.getTrustedProxies());
        } else {
            return create(network.getRemoteIpHttpHeader(), network.getTrustedProxiesPattern());
        }
    }

    private static final IPAddressGenerator ipAddressGenerator = new IPAddressGenerator();
    private static final Splitter splitter = Splitter.on(',').trimResults();

    private static final Logger log = LogManager.getLogger(ClientAddressAscertainer.class);

    public ClientIpInfo getActualRemoteAddress(RestRequest request) {
        HttpRequest httpRequest = request.getHttpRequest();
        HttpChannel httpChannel = request.getHttpChannel();
        return getActualRemoteAddress(httpRequest, httpChannel);
    }

    public abstract ClientIpInfo getActualRemoteAddress(HttpRequest request, HttpChannel httpChannel);

    static class CIDRBased extends ClientAddressAscertainer {

        private final String remoteIpHeader;
        private final IPAddressCollection trustedProxies;

        CIDRBased(String remoteIpHeader, IPAddressCollection trustedProxies) {
            this.remoteIpHeader = remoteIpHeader;
            this.trustedProxies = trustedProxies;
        }

        @Override
        public ClientIpInfo getActualRemoteAddress(HttpRequest request, HttpChannel httpChannel) {
            IPAddress directIpAddress = ipAddressGenerator.from(httpChannel.getRemoteAddress().getAddress());

            if (!trustedProxies.contains(directIpAddress)) {
                if (log.isDebugEnabled()) {
                    log.debug("Request from untrusted host: " + directIpAddress);
                }
                return ClientIpInfo.untrusted(directIpAddress, httpChannel.getRemoteAddress());
            }

            List<String> xffHeaders = request.getHeaders().get(remoteIpHeader);

            if (xffHeaders == null || xffHeaders.isEmpty()) {
                return ClientIpInfo.trusted(directIpAddress, directIpAddress, httpChannel.getRemoteAddress());
            }

            List<IPAddressString> ipAddressStrings = xffHeaders.stream().flatMap(h -> splitter.splitToStream(h)).map(ip -> new IPAddressString(ip))
                    .collect(Collectors.toList());

            // From right to left, find first untrusted IP. This will be our new client address

            for (IPAddressString ipString : Lists.reverse(ipAddressStrings)) {
                try {
                    IPAddress ip = ipString.toAddress();

                    if (!trustedProxies.contains(ip)) {
                        if (log.isDebugEnabled()) {
                            log.debug("Request from trusted proxy " + directIpAddress + "; actual client: " + ip);
                        }
                        return ClientIpInfo.trusted(directIpAddress, ip, httpChannel.getRemoteAddress());
                    }
                } catch (AddressStringException | IncompatibleAddressException e) {
                    log.warn("Unparseable IP in XFF headers of request: " + xffHeaders, e);
                    throw new ElasticsearchStatusException("Invalid " + remoteIpHeader + "header", RestStatus.BAD_REQUEST);
                }
            }

            // If we got here, this is a weird case: All IPs are trusted. Well, let's take the last one anyway.

            if (log.isDebugEnabled()) {
                log.debug("Request from trusted proxy " + directIpAddress + "; actual client: " + ipAddressStrings.get(0) + " (which is also trusted)");
            }

            try {
                return ClientIpInfo.trusted(directIpAddress, ipAddressStrings.get(0).toAddress(), httpChannel.getRemoteAddress());
            } catch (AddressStringException | IncompatibleAddressException e) {
                log.warn("Unparseable IP in XFF headers of request: " + xffHeaders, e);
                throw new ElasticsearchStatusException("Invalid " + remoteIpHeader + "header", RestStatus.BAD_REQUEST);
            }
        }
    }

    /**
     * @deprecated just for supporting legacy configuration
     */
    @Deprecated
    static class PatternBased extends ClientAddressAscertainer {
        private final String remoteIpHeader;
        private final Pattern trustedProxiesPattern;

        PatternBased(String remoteIpHeader, Pattern trustedProxiesPattern) {
            this.remoteIpHeader = remoteIpHeader;
            this.trustedProxiesPattern = trustedProxiesPattern;

        }

        @Override
        public ClientIpInfo getActualRemoteAddress(HttpRequest request, HttpChannel httpChannel) {
            IPAddress directIpAddress = ipAddressGenerator.from(httpChannel.getRemoteAddress().getAddress());
            String ipAddressString = httpChannel.getRemoteAddress().getAddress().getHostAddress();

            if (!trustedProxiesPattern.matcher(ipAddressString).matches()) {
                if (log.isDebugEnabled()) {
                    log.debug("Request from untrusted host: " + directIpAddress);
                }
                return ClientIpInfo.untrusted(directIpAddress, httpChannel.getRemoteAddress());
            }
            List<String> xffHeaders = request.getHeaders().get(remoteIpHeader);

            if (xffHeaders == null || xffHeaders.isEmpty()) {
                return ClientIpInfo.trusted(directIpAddress, directIpAddress, httpChannel.getRemoteAddress());
            }

            List<IPAddressString> ipAddressStrings = xffHeaders.stream().flatMap(h -> splitter.splitToStream(h)).map(ip -> new IPAddressString(ip))
                    .collect(Collectors.toList());

            // From right to left, find first untrusted IP. This will be our new client address

            for (IPAddressString ipString : Lists.reverse(ipAddressStrings)) {
                try {
                    IPAddress ip = ipString.toAddress();

                    if (!trustedProxiesPattern.matcher(ipString.toString()).matches()) {
                        if (log.isDebugEnabled()) {
                            log.debug("Request from trusted proxy " + directIpAddress + "; actual client: " + ip);
                        }

                        return ClientIpInfo.trusted(directIpAddress, ip, httpChannel.getRemoteAddress());
                    }
                } catch (AddressStringException | IncompatibleAddressException e) {
                    log.warn("Unparseable IP in XFF headers of request: " + xffHeaders, e);
                    throw new ElasticsearchStatusException("Invalid " + remoteIpHeader + "header", RestStatus.BAD_REQUEST);
                }
            }

            // If we got here, this is a weird case: All IPs are trusted. Well, let's take the last one anyway.
            if (log.isDebugEnabled()) {
                log.debug("Request from trusted proxy " + directIpAddress + "; actual client: " + ipAddressStrings.get(0) + " (which is also trusted)");
            }

            try {
                return ClientIpInfo.trusted(directIpAddress, ipAddressStrings.get(0).toAddress(), httpChannel.getRemoteAddress());
            } catch (AddressStringException | IncompatibleAddressException e) {
                log.warn("Unparseable IP in XFF headers of request: " + xffHeaders, e);
                throw new ElasticsearchStatusException("Invalid " + remoteIpHeader + "header", RestStatus.BAD_REQUEST);
            }
        }
    }

    static class Inactive extends ClientAddressAscertainer {

        @Override
        public ClientIpInfo getActualRemoteAddress(HttpRequest request, HttpChannel httpChannel) {
            return ClientIpInfo.untrusted(ipAddressGenerator.from(httpChannel.getRemoteAddress().getAddress()),
                    httpChannel.getRemoteAddress());
        }
    }

    public static class ClientIpInfo {
        private final IPAddress directIpAddress;
        private final IPAddress originatingIpAddress;
        private final boolean trustedProxy;
        private final InetSocketAddress originalRemoteAddress;

        ClientIpInfo(IPAddress directIpAddress, IPAddress originatingIpAddress, boolean trustedProxy, InetSocketAddress originalRemoteAddress) {
            this.directIpAddress = directIpAddress;
            this.originatingIpAddress = originatingIpAddress;
            this.trustedProxy = trustedProxy;
            this.originalRemoteAddress = originalRemoteAddress;
        }

        public IPAddress getDirectIpAddress() {
            return directIpAddress;
        }
        
        public IPAddress getOriginatingIpAddress() {
            return originatingIpAddress;
        }

        /**
         * Just for backwards compatibility with other SG components
         */
        public TransportAddress getOriginatingTransportAddress() {
            return new TransportAddress(new InetSocketAddress(originatingIpAddress.toInetAddress(), originalRemoteAddress.getPort()));
        }

        public boolean isTrustedProxy() {
            return trustedProxy;
        }

        static ClientIpInfo trusted(IPAddress directIpAddress, IPAddress originatingIpAddress, InetSocketAddress originalRemoteAddress) {
            return new ClientIpInfo(directIpAddress, originatingIpAddress, true, originalRemoteAddress);
        }

        static ClientIpInfo untrusted(IPAddress ipAddress, InetSocketAddress originalRemoteAddress) {
            return new ClientIpInfo(ipAddress, ipAddress, false, originalRemoteAddress);
        }

        @Override
        public String toString() {
            return "ClientInfo [directIpAddress=" + directIpAddress + ", originatingIpAddress=" + originatingIpAddress + ", trustedProxy="
                    + trustedProxy + "]";
        }

     

    }
}
