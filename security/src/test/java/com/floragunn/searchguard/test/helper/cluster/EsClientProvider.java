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
import java.util.function.Consumer;

import javax.net.ssl.SSLContext;

import org.apache.http.Header;
import org.apache.http.message.BasicHeader;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.helper.certificate.TestCertificate;
import com.floragunn.searchguard.test.helper.certificate.TestCertificates;

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
        ImmutableList<Header> headersList = ImmutableList.ofArray(headers);

        if (user.isAdminCertUser()) {
            return getAdminCertRestClient(user, headersList);
        } else {
            return getRestClient(user, headersList.with(getBasicAuthHeader(user.getName(), user.getPassword())));
        }
    }

    default GenericRestClient getRestClient(String user, String password, String tenant) {
        return getRestClient(user, password, new BasicHeader("sgtenant", tenant));
    }

    default GenericRestClient getRestClient(String user, String password, Header... headers) {
        return getRestClient(UserCredentialsHolder.basic(user, password), headers);
    }

    default GenericRestClient getRestClient(Header... headers) {
        return getRestClient(null, Arrays.asList(headers));
    }

    default GenericRestClient getRestClient(UserCredentialsHolder user, List<Header> headers) {
        return new GenericRestClient(getHttpAddress(), headers, getAnyClientSslContextProvider().getSslContext(false), user,
                getRequestInfoConsumer());
    }

    default GenericRestClient getAdminCertRestClient() {
        return getAdminCertRestClient(UserCredentialsHolder.ADMIN, ImmutableList.empty());
    }

    default GenericRestClient getAdminCertRestClient(UserCredentialsHolder user, List<Header> headers) {
        return new GenericRestClient(getHttpAddress(), headers, getAdminClientSslContextProvider().getSslContext(true), user,
                getRequestInfoConsumer());
    }

    default GenericRestClient getUserCertRestClient(String subject, Header... headers) {
        SSLContext sslContext = getUserClientSslContextProvider(subject).getSslContext(true);
        return new GenericRestClient(getHttpAddress(), Arrays.asList(headers), sslContext, UserCredentialsHolder.basic(subject, null),
                getRequestInfoConsumer());
    }
    
    default GenericRestClient getRestClientWithoutTls(Header... headers) {
        return getRestClientWithoutTls(Arrays.asList(headers));
    }

    
    default GenericRestClient getRestClientWithoutTls(List<Header> headers) {
        return new GenericRestClient(getHttpAddress(), headers, null, null,
                getRequestInfoConsumer());
    }

    default BasicHeader getBasicAuthHeader(String user, String password) {
        return new BasicHeader("Authorization",
                "Basic " + Base64.getEncoder().encodeToString((user + ":" + Objects.requireNonNull(password)).getBytes(StandardCharsets.UTF_8)));
    }

    Consumer<GenericRestClient.RequestInfo> getRequestInfoConsumer();

    public interface UserCredentialsHolder {
        String getName();

        String getPassword();

        default boolean isAdminCertUser() {
            return false;
        }

        static final UserCredentialsHolder ADMIN = new UserCredentialsHolder() {

            @Override
            public String getName() {
                return "<admin cert user>";
            }

            @Override
            public String getPassword() {
                return null;
            }

            @Override
            public boolean isAdminCertUser() {
                return true;
            }

            @Override
            public String toString() {
                return getName();
            }
        };

        static UserCredentialsHolder basic(String name, String password) {
            return new UserCredentialsHolder() {

                @Override
                public String getName() {
                    return name;
                }

                @Override
                public String getPassword() {
                    return password;
                }

                @Override
                public String toString() {
                    return getName();
                }
            };
        }
    }

}
