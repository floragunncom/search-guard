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

package com.floragunn.searchguard.ssl.rest;

import static org.elasticsearch.rest.RestRequest.Method.GET;

import java.io.IOException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;

import com.floragunn.searchguard.ssl.SearchGuardKeyStore;
import com.floragunn.searchguard.ssl.transport.PrincipalExtractor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class SearchGuardSSLInfoAction extends BaseRestHandler {

    private final Logger log = LogManager.getLogger(this.getClass());
    private final ThreadPool threadPool;
    private final SearchGuardKeyStore sgks;
    final PrincipalExtractor principalExtractor;

    public SearchGuardSSLInfoAction(ThreadPool threadPool, final SearchGuardKeyStore sgks, final PrincipalExtractor principalExtractor) {
        super();
        this.threadPool = threadPool;
        this.sgks = sgks;
        this.principalExtractor = principalExtractor;
    }
    
    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(GET, "/_searchguard/sslinfo"));
    }
    
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return new RestChannelConsumer() {
            
            final Boolean showDn = request.paramAsBoolean("show_dn", Boolean.FALSE);
            final Boolean showServerCerts = request.paramAsBoolean("show_server_certs", Boolean.FALSE);
            final Boolean showFullServerCerts = request.paramAsBoolean("show_full_server_certs", Boolean.FALSE);

            @Override
            public void accept(RestChannel channel) throws Exception {
                XContentBuilder builder = channel.newBuilder();

                final ThreadContext threadContext = threadPool.getThreadContext();

                X509Certificate[] certs = threadContext.getTransient(SSLConfigConstants.SG_SSL_PEER_CERTIFICATES);
                if (certs != null) {
                    certs = certs.clone();
                }
                X509Certificate[] localCerts = threadContext.getTransient(SSLConfigConstants.SG_SSL_LOCAL_CERTIFICATES);
                if (localCerts != null) {
                    localCerts = localCerts.clone();
                }
                String principal = threadContext.getTransient(SSLConfigConstants.SG_SSL_PRINCIPAL);
                String protocol = threadContext.getTransient(SSLConfigConstants.SG_SSL_PROTOCOL);
                String cipher = threadContext.getTransient(SSLConfigConstants.SG_SSL_CIPHER);

                builder.startObject();

                builder.field("principal", principal);
                builder.field("peer_certificates", certs != null && certs.length > 0 ? certs.length + "" : "0");

                if(showDn == Boolean.TRUE) {
                    builder.field("peer_certificates_list", certs == null?null:Arrays.stream(certs).map(c->c.getSubjectDN().getName()).collect(Collectors.toList()));
                    builder.field("local_certificates_list", localCerts == null?null:Arrays.stream(localCerts).map(c->c.getSubjectDN().getName()).collect(Collectors.toList()));
                }

                builder.field("ssl_protocol", protocol);
                builder.field("ssl_cipher", cipher);

                builder.field("ssl_openssl_available", false);
                builder.field("ssl_openssl_version", -1);
                builder.field("ssl_openssl_version_string", (String) null);
                builder.field("ssl_openssl_non_available_cause", "Not supported any longer");
                builder.field("ssl_openssl_supports_key_manager_factory", false);
                builder.field("ssl_openssl_supports_hostname_validation", false);

                if (showServerCerts == Boolean.TRUE || showFullServerCerts == Boolean.TRUE) {
                    if (sgks != null) {
                        builder.field("http_certificates_list", generateCertDetailList(sgks.getHttpCerts(), showFullServerCerts));
                        builder.field("transport_certificates_list", generateCertDetailList(sgks.getTransportCerts(), showFullServerCerts));
                    } else {
                        builder.field("message", "keystore is not initialized");
                    }
                }

                builder.field("ssl_provider_http", sgks.getHTTPProviderName());
                builder.field("ssl_provider_transport_server", sgks.getTransportServerProviderName());
                builder.field("ssl_provider_transport_client", sgks.getTransportClientProviderName());
                builder.endObject();

                builder.close();

                channel.sendResponse(new RestResponse(RestStatus.OK, builder));
            }
        };
    }

    private List<Map<String, String>> generateCertDetailList(final X509Certificate[] certs, final boolean fullChain) {
        if (certs == null) {
            return null;
        }
        
        return Arrays
        		.stream(certs)
        		.limit(fullChain?certs.length:1)
        		.map(cert -> {
                    final String issuerDn = cert != null && cert.getIssuerX500Principal() != null ? cert.getIssuerX500Principal().getName(): "";
                    final String subjectDn = cert != null && cert.getSubjectX500Principal() != null ? cert.getSubjectX500Principal().getName(): "";

                    String san = "";
                    try {
                        san = cert !=null && cert.getSubjectAlternativeNames() != null ? cert.getSubjectAlternativeNames().toString() : "";
                    } catch (CertificateParsingException e) {
                        log.error("Issue parsing SubjectAlternativeName:", e);
                    }

                    final String notBefore = cert != null && cert.getNotBefore() != null ? cert.getNotBefore().toInstant().toString(): "";
                    final String notAfter = cert != null && cert.getNotAfter() != null ? cert.getNotAfter().toInstant().toString(): "";
                    return ImmutableMap.<String, String>builder()
                            .put("issuer_dn", issuerDn)
                            .put("subject_dn", subjectDn)
                            .put("san", san)
                            .put("not_before", notBefore)
                            .put("not_after", notAfter)
                            .build();
                })
                .collect(Collectors.toList());
    }

    @Override
    public String getName() {
        return "Search Guard SSL Info";
    }
}
