/* This product includes software developed by Amazon.com, Inc.
 * (https://github.com/opendistro-for-elasticsearch/security)
 *
 * Copyright 2015-2020 floragunn GmbH
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

package com.floragunn.searchguard.rest;

import static org.elasticsearch.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;

import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.ssl.SearchGuardKeyStore;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;

public class SSLReloadCertAction extends BaseRestHandler {
    private static final List<Route> routes = Collections.singletonList(new Route(POST, "_searchguard/api/ssl/{certType}/reloadcerts/"));

    private final SearchGuardKeyStore keyStore;
    private final ThreadContext threadContext;
    private final boolean sslCertReloadEnabled;
    private final AdminDNs adminDns;

    public SSLReloadCertAction(final SearchGuardKeyStore keyStore, final ThreadPool threadPool, final AdminDNs adminDns, boolean sslCertReloadEnabled) {
        this.keyStore = keyStore;
        this.adminDns = adminDns;
        this.threadContext = threadPool.getThreadContext();
        this.sslCertReloadEnabled = sslCertReloadEnabled;
    }

    @Override
    public List<Route> routes() {
        return routes;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return new RestChannelConsumer() {
            final String certType = request.param("certType").toLowerCase().trim();

            @Override
            public void accept(RestChannel channel) throws Exception {
                if (!sslCertReloadEnabled) {
                    BytesRestResponse response = new BytesRestResponse(RestStatus.BAD_REQUEST,
                            "SSL Reload action called while " + ConfigConstants.SEARCHGUARD_SSL_CERT_RELOAD_ENABLED + " is set to false.");
                    channel.sendResponse(response);
                    return;
                }

                XContentBuilder builder = channel.newBuilder();
                BytesRestResponse response;

                // Check for Super admin user
                final User user = threadContext.getTransient(ConfigConstants.SG_USER);
                if (user == null || !adminDns.isAdmin(user)) {
                    response = new BytesRestResponse(RestStatus.FORBIDDEN, "");
                } else {
                    try {
                        builder.startObject();
                        if (keyStore != null) {
                            switch (certType) {
                                case "http":
                                    keyStore.initHttpSSLConfig();
                                    builder.field("message", "updated http certs");
                                    builder.endObject();
                                    response = new BytesRestResponse(RestStatus.OK, builder);
                                    break;
                                case "transport":
                                    keyStore.initTransportSSLConfig();
                                    builder.field("message", "updated transport certs");
                                    builder.endObject();
                                    response = new BytesRestResponse(RestStatus.OK, builder);
                                    break;
                                default:
                                    builder.field("message", "invalid uri path, please use /_searchguard/api/ssl/http/reload or "
                                            + "/_searchguard/api/ssl/transport/reload");
                                    builder.endObject();
                                    response = new BytesRestResponse(RestStatus.FORBIDDEN, builder);
                                    break;
                            }
                        } else {
                            builder.field("message", "keystore is not initialized");
                            builder.endObject();
                            response = new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, builder);
                        }
                    } catch (final Exception e1) {
                        builder = channel.newBuilder();
                        builder.startObject();
                        builder.field("error", e1.toString());
                        builder.endObject();
                        response = new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, builder);
                    } finally {
                        if (builder != null) {
                            builder.close();
                        }
                    }
                }
                channel.sendResponse(response);
            }
        };
    }

    @Override
    public String getName() {
        return "SSL Cert Reload Action";
    }
}