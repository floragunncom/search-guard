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

import static org.opensearch.rest.RestRequest.Method.GET;

import java.io.IOException;
import java.nio.file.Path;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;

import com.floragunn.searchguard.ssl.SearchGuardKeyStore;
import com.floragunn.searchguard.ssl.transport.PrincipalExtractor;
import com.floragunn.searchguard.ssl.util.SSLRequestHelper;
import com.floragunn.searchguard.ssl.util.SSLRequestHelper.SSLInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class SearchGuardSSLInfoAction extends BaseRestHandler {

    private final Logger log = LogManager.getLogger(this.getClass());
    private final SearchGuardKeyStore sgks;
    final PrincipalExtractor principalExtractor;
    private final Path configPath;
    private final Settings settings;

    public SearchGuardSSLInfoAction(final Settings settings, final Path configPath, final RestController controller,
            final SearchGuardKeyStore sgks, final PrincipalExtractor principalExtractor) {
        super();
        this.settings = settings;
        this.sgks = sgks;
        this.principalExtractor = principalExtractor;
        this.configPath = configPath;
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
                BytesRestResponse response = null;

                try {
                    
                    SSLInfo sslInfo = SSLRequestHelper.getSSLInfo(settings, configPath, request, principalExtractor);
                    X509Certificate[] certs = sslInfo == null?null:sslInfo.getX509Certs();
                    X509Certificate[] localCerts = sslInfo == null?null:sslInfo.getLocalCertificates();

                    builder.startObject();

                    builder.field("principal", sslInfo == null?null:sslInfo.getPrincipal());
                    builder.field("peer_certificates", certs != null && certs.length > 0 ? certs.length + "" : "0");

                    if(showDn == Boolean.TRUE) {
                        builder.field("peer_certificates_list", certs == null?null:Arrays.stream(certs).map(c->c.getSubjectDN().getName()).collect(Collectors.toList()));
                        builder.field("local_certificates_list", localCerts == null?null:Arrays.stream(localCerts).map(c->c.getSubjectDN().getName()).collect(Collectors.toList()));
                    }

                    builder.field("ssl_protocol", sslInfo == null?null:sslInfo.getProtocol());
                    builder.field("ssl_cipher", sslInfo == null?null:sslInfo.getCipher());
                                      
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

                    response = new BytesRestResponse(RestStatus.OK, builder);
                } catch (final Exception e1) {
                    log.error("Error handle request "+e1, e1);
                    builder = channel.newBuilder();
                    builder.startObject();
                    builder.field("error", e1.toString());
                    builder.endObject();
                    response = new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, builder);
                } finally {
                    if(builder != null) {
                        builder.close();
                    }
                }
                
                channel.sendResponse(response);
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
