/*
 * Copyright 2020 floragunn GmbH
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

package com.floragunn.searchguard.test.helper.cluster;

import com.floragunn.searchguard.test.helper.rest.GenericRestClient;
import com.floragunn.searchguard.test.helper.rest.StaticCertificatesBasedSSLContextProvider;
import org.apache.http.*;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.protocol.HttpContext;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;

public interface EsClientProvider {

    default GenericRestClient createGenericClientRestClient(InetSocketAddress nodeHttpAddress, List<Header> headers) {
        return new GenericRestClient(nodeHttpAddress, headers, new StaticCertificatesBasedSSLContextProvider(getResourceFolder(), "node-0-keystore.jks", "truststore.jks", false, true));
    }

    default GenericRestClient createGenericAdminRestClient(InetSocketAddress nodeHttpAddress, List<Header> headers) {
        return new GenericRestClient(nodeHttpAddress, headers, new StaticCertificatesBasedSSLContextProvider(getResourceFolder(), "kirk-keystore.jks", "truststore.jks", true, true));
    }

    default GenericRestClient getRestClient(TestSgConfig.User user, Header... headers) {
        return getRestClient(user.getName(), user.getPassword(), headers);
    }

    default GenericRestClient getRestClient(String user, String password, String tenant) {
        BasicHeader basicAuthHeader = new BasicHeader("Authorization",
                "Basic " + Base64.getEncoder().encodeToString((user + ":" + Objects.requireNonNull(password)).getBytes(StandardCharsets.UTF_8)));

        return createGenericClientRestClient(getHttpAddress(), Arrays.asList(basicAuthHeader, new BasicHeader("sgtenant", tenant)));
    }

    default GenericRestClient getRestClient(String user, String password, Header... headers) {
        BasicHeader basicAuthHeader = new BasicHeader("Authorization",
                "Basic " + Base64.getEncoder().encodeToString((user + ":" + Objects.requireNonNull(password)).getBytes(StandardCharsets.UTF_8)));

        List<Header> headersList = new ArrayList<>();
        headersList.add(basicAuthHeader);
        if (headers != null && headers.length > 0) {
            headersList.addAll(Arrays.asList(headers));
        }

        return createGenericClientRestClient(getHttpAddress(), headersList);
    }

    default GenericRestClient getRestClient(Header... headers) {
        return createGenericClientRestClient(getHttpAddress(), Arrays.asList(headers));
    }

    default GenericRestClient getAdminCertRestClient() {
        return createGenericAdminRestClient(getHttpAddress(), Collections.emptyList());
    }

    default RestHighLevelClient getRestHighLevelClient(TestSgConfig.User user) {
        return getRestHighLevelClient(user.getName(), user.getPassword());
    }

    default RestHighLevelClient getRestHighLevelClient(String user, String password) {
        return getRestHighLevelClient(user, password, null);
    }

    //todo
    default RestHighLevelClient getRestHighLevelClient(String user, String password, String tenant) {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));

        HttpClientConfigCallback configCallback = new HttpClientConfigCallback() {

            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                httpClientBuilder = httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider).setSSLStrategy(getSSLIOSessionStrategy());

                if (tenant != null) {
                    httpClientBuilder = httpClientBuilder.addInterceptorLast(new HttpRequestInterceptor() {

                        @Override
                        public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
                            request.setHeader("sgtenant", tenant);

                        }

                    });
                }

                return httpClientBuilder;
            }
        };

        RestClientBuilder builder = RestClient.builder(new HttpHost(getHttpAddress().getHostString(), getHttpAddress().getPort(), "https"))
                .setHttpClientConfigCallback(configCallback);

        return new RestHighLevelClient(builder);
    }

    //todo
    default RestHighLevelClient getRestHighLevelClient(Header... headers) {
        RestClientBuilder builder = RestClient.builder(new HttpHost(getHttpAddress().getHostString(), getHttpAddress().getPort(), "https"))
                .setDefaultHeaders(headers)
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setSSLStrategy(getSSLIOSessionStrategy()));

        return new RestHighLevelClient(builder);
    }

    @Deprecated
    default Client getAdminCertClient() {
        String prefix = getResourceFolder() == null ? "" : getResourceFolder() + "/";

        return new LocalEsClusterTransportClient(getClusterName(), getTransportAddress(), prefix + "truststore.jks", prefix + "kirk-keystore.jks");
    }

    Client getInternalNodeClient();

    InetSocketAddress getHttpAddress();

    InetSocketAddress getTransportAddress();

    String getClusterName();

    // XXX fixme
    String getResourceFolder();

    SSLIOSessionStrategy getSSLIOSessionStrategy();
}
