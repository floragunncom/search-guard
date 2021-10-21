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

import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient;
import com.floragunn.searchguard.test.helper.rest.SSLContextProvider;
import com.floragunn.searchguard.test.helper.rest.TestCertificateBasedSSLContextProvider;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestHighLevelClient;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EsClientProvider {

    private final String clusterName;
    private final TestCertificates testCertificates;
    private final SSLContextProvider adminClientSslContextProvider;
    private final SSLContextProvider anyClientSslContextProvider;

    public EsClientProvider(String clusterName, TestCertificates testCertificates) {
        this.clusterName = clusterName;
        this.testCertificates = testCertificates;
        this.adminClientSslContextProvider = new TestCertificateBasedSSLContextProvider(testCertificates.getCaCertificate(), testCertificates.getAdminCertificate());
        this.anyClientSslContextProvider = new TestCertificateBasedSSLContextProvider(testCertificates.getCaCertificate(), testCertificates.getAdminCertificate());
    }

    public GenericRestClient getRestClient(InetSocketAddress httpAddress, TestSgConfig.User user, Header... headers) {
        return getRestClient(httpAddress, user.getName(), user.getPassword(), headers);
    }

    public GenericRestClient getRestClient(InetSocketAddress httpAddress, String user, String password, String tenant) {
        return getRestClient(httpAddress, user, password, new BasicHeader("sgtenant", tenant));
    }

    public GenericRestClient getRestClient(InetSocketAddress httpAddress, String user, String password, Header... headers) {
        BasicHeader basicAuthHeader = getBasicAuthHeader(user, password);
        if (headers != null && headers.length > 0) {
            List<Header> concatenatedHeaders = Stream.concat(Stream.of(basicAuthHeader), Stream.of(headers))
                    .collect(Collectors.toList());
            return getRestClient(httpAddress, concatenatedHeaders);
        }
        return getRestClient(httpAddress, basicAuthHeader);
    }

    public GenericRestClient getRestClient(InetSocketAddress httpAddress, Header... headers) {
        return getRestClient(httpAddress, Arrays.asList(headers));
    }

    public GenericRestClient getRestClient(InetSocketAddress httpAddress, List<Header> headers) {
        return createGenericClientRestClient(httpAddress, headers);
    }

    public GenericRestClient getAdminCertRestClient(InetSocketAddress httpAddress) {
        return createGenericAdminRestClient(httpAddress, Collections.emptyList());
    }

    public RestHighLevelClient getRestHighLevelClient(InetSocketAddress httpAddress, TestSgConfig.User user) {
        return getRestHighLevelClient(httpAddress, user.getName(), user.getPassword());
    }

    public RestHighLevelClient getRestHighLevelClient(InetSocketAddress httpAddress, String user, String password) {
        return getRestHighLevelClient(httpAddress, user, password, null);
    }

    public RestHighLevelClient getRestHighLevelClient(InetSocketAddress httpAddress, String user, String password, String tenant) {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));


        HttpClientConfigCallback configCallback = httpClientBuilder -> {
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                    .setSSLStrategy(new SSLIOSessionStrategy(anyClientSslContextProvider.getSslContext(false), null,
                            null, NoopHostnameVerifier.INSTANCE));

            if (tenant != null) {
                httpClientBuilder.addInterceptorLast((HttpRequestInterceptor) (request, context) -> request.setHeader("sgtenant", tenant));
            }

            return httpClientBuilder;
        };

        RestClientBuilder builder = RestClient.builder(new HttpHost(httpAddress.getHostString(), httpAddress.getPort(), "https"))
                .setHttpClientConfigCallback(configCallback);

        return new RestHighLevelClient(builder);
    }

    public RestHighLevelClient getRestHighLevelClient(InetSocketAddress httpAddress, Header... headers) {
        RestClientBuilder builder = RestClient.builder(new HttpHost(httpAddress.getHostString(), httpAddress.getPort(), "https"))
                .setDefaultHeaders(headers)
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                        .setSSLStrategy(new SSLIOSessionStrategy(anyClientSslContextProvider.getSslContext(false), null, null,
                                NoopHostnameVerifier.INSTANCE)));

        return new RestHighLevelClient(builder);
    }

    @Deprecated
    Client getAdminCertClient(InetSocketAddress transportAddress) {
        return new LocalEsClusterTransportClient(clusterName, transportAddress, testCertificates.getAdminCertificate(), testCertificates.getCaCertFile().toPath());
    }

    private GenericRestClient createGenericClientRestClient(InetSocketAddress nodeHttpAddress, List<Header> headers) {
        return new GenericRestClient(nodeHttpAddress, headers, anyClientSslContextProvider.getSslContext(false));
    }

    private GenericRestClient createGenericAdminRestClient(InetSocketAddress nodeHttpAddress, List<Header> headers) {
        //a client authentication is needed for admin because admin needs to authenticate itself (dn matching in config file)
        return new GenericRestClient(nodeHttpAddress, headers, adminClientSslContextProvider.getSslContext(true));
    }

    private BasicHeader getBasicAuthHeader(String user, String password) {
        return new BasicHeader("Authorization",
                "Basic " + Base64.getEncoder().encodeToString((user + ":" + Objects.requireNonNull(password)).getBytes(StandardCharsets.UTF_8)));
    }

}
