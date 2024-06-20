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

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.floragunn.searchguard.test.helper.certificate.TestCertificate;
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

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.helper.certificate.TestCertificates;

import javax.net.ssl.SSLContext;

public interface EsClientProvider {

    String getClusterName();

    TestCertificates getTestCertificates();

    InetSocketAddress getHttpAddress();

    InetSocketAddress getTransportAddress();

    default URI getHttpAddressAsURI() {
        InetSocketAddress address = getHttpAddress();
        return URI.create("https://" + address.getHostString() + ":" + address.getPort());
    }

    default SSLContextProvider getAdminClientSslContextProvider() {
        return new TestCertificateBasedSSLContextProvider(getTestCertificates().getCaCertificate(), getTestCertificates().getAdminCertificate());
    }

    default SSLContextProvider getUserClientSslContextProvider(String subjectDistinguishedName) {
        TestCertificate userCertificate = getTestCertificates().create(subjectDistinguishedName);
        return new TestCertificateBasedSSLContextProvider(getTestCertificates().getCaCertificate(), userCertificate);
    }

    default SSLContextProvider getAnyClientSslContextProvider() {
        return new TestCertificateBasedSSLContextProvider(getTestCertificates().getCaCertificate(), getTestCertificates().getAnyClientCertificate());
    }

    default GenericRestClient getRestClient(UserCredentialsHolder user, Header... headers) {
        if (user.isAdminCertUser()) {
            return getAdminCertRestClient();
        } else {
            return getRestClient(user.getName(), user.getPassword(), headers);   
        }        
    }

    default GenericRestClient getRestClient(String user, String password, String tenant) {
        return getRestClient(user, password, new BasicHeader("sgtenant", tenant));
    }

    default GenericRestClient getRestClient(String user, String password, Header... headers) {
        BasicHeader basicAuthHeader = getBasicAuthHeader(user, password);
        if (headers != null && headers.length > 0) {
            List<Header> concatenatedHeaders = Stream.concat(Stream.of(basicAuthHeader), Stream.of(headers)).collect(Collectors.toList());
            return getRestClient(concatenatedHeaders);
        }
        return getRestClient(basicAuthHeader);
    }

    default GenericRestClient getRestClient(Header... headers) {
        return getRestClient(Arrays.asList(headers));
    }

    default GenericRestClient getRestClient(List<Header> headers) {
        return new GenericRestClient(getHttpAddress(), headers, getAnyClientSslContextProvider().getSslContext(false));
    }

    default GenericRestClient getAdminCertRestClient() {
        return getAdminCertRestClient(ImmutableList.empty());
    }

    default GenericRestClient getAdminCertRestClient(List<Header> headers) {
        return new GenericRestClient(getHttpAddress(), headers, getAdminClientSslContextProvider().getSslContext(true));
    }

    default GenericRestClient getUserCertRestClient(String subject, Header...headers) {
        SSLContext sslContext = getUserClientSslContextProvider(subject).getSslContext(true);
        return new GenericRestClient(getHttpAddress(), Arrays.asList(headers), sslContext);
    }

    default RestHighLevelClient getRestHighLevelClient(UserCredentialsHolder user) {
        return getRestHighLevelClient(user.getName(), user.getPassword());
    }

    default RestHighLevelClient getRestHighLevelClient(String user, String password) {
        return getRestHighLevelClient(user, password, null);
    }

    default RestHighLevelClient getRestHighLevelClient(String user, String password, String tenant) {
        InetSocketAddress httpAddress = getHttpAddress();
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));

        HttpClientConfigCallback configCallback = httpClientBuilder -> {
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider).setSSLStrategy(
                    new SSLIOSessionStrategy(getAnyClientSslContextProvider().getSslContext(false), null, null, NoopHostnameVerifier.INSTANCE));

            if (tenant != null) {
                httpClientBuilder.addInterceptorLast((HttpRequestInterceptor) (request, context) -> request.setHeader("sgtenant", tenant));
            }

            return httpClientBuilder;
        };

        RestClientBuilder builder = RestClient.builder(new HttpHost(httpAddress.getHostString(), httpAddress.getPort(), "https"))
                .setHttpClientConfigCallback(configCallback);

        return new RestHighLevelClient(builder);
    }

    default RestHighLevelClient getRestHighLevelClient(Header... headers) {
        return new RestHighLevelClient(getLowLevelRestClientBuilder(headers));
    }

    default RestClientBuilder getLowLevelRestClientBuilder(Header... headers) {
        InetSocketAddress httpAddress = getHttpAddress();
        return RestClient.builder(new HttpHost(httpAddress.getHostString(), httpAddress.getPort(), "https"))
                .setDefaultHeaders(headers).setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setSSLStrategy(
                        new SSLIOSessionStrategy(getAnyClientSslContextProvider().getSslContext(false), null, null, NoopHostnameVerifier.INSTANCE)));
    }
    
    default RestClient getLowLevelRestClient(Header... headers) {
        return getLowLevelRestClientBuilder(headers).build();
    }
    
    @Deprecated
    default Client getAdminCertClient() {
        return new LocalEsClusterTransportClient(getClusterName(), getTransportAddress(), getTestCertificates().getAdminCertificate(),
                getTestCertificates().getCaCertFile().toPath());
    }

    default BasicHeader getBasicAuthHeader(String user, String password) {
        return new BasicHeader("Authorization",
                "Basic " + Base64.getEncoder().encodeToString((user + ":" + Objects.requireNonNull(password)).getBytes(StandardCharsets.UTF_8)));
    }

    public interface UserCredentialsHolder {
        String getName();

        String getPassword();
        
        default boolean isAdminCertUser() {
            return false;
        }
    }

}
