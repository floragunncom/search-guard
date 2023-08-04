/*
 * Copyright 2020-2023 floragunn GmbH
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
package com.floragunn.signals.watch.common;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.function.Supplier;

import static com.floragunn.searchsupport.junit.ThrowableAssert.assertThatThrown;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RefreshableX509TrustManagerTest {

    public static final String AUTH_TYPE_1 = "auth-type-1";
    public static final String AUTH_TYPE_2 = "auth-type-2";
    public static final String AUTH_TYPE_3 = "auth-type-3";
    public static final X509Certificate[] CHAIN_OF_TRUST_1 = new X509Certificate[0];
    public static final X509Certificate[] CHAIN_OF_TRUST_2 = new X509Certificate[1];
    public static final X509Certificate[] CHAIN_OF_TRUST_3 = new X509Certificate[3];

    @Mock
    private X509ExtendedTrustManager mainTrustManagerOne;
    @Mock
    private X509ExtendedTrustManager mainTrustManagerTwo;
    @Mock
    private X509ExtendedTrustManager mainTrustManagerThree;

    @Mock
    private SSLEngine engine;

    @Mock
    private Socket socket;

    // under tests
    private RefreshableX509TrustManager trustManager;

    @Before
    public void before() {
        this.trustManager = new RefreshableX509TrustManager("test-trust-manager", () -> mainTrustManagerOne);
    }

    @Test
    public void shouldDelegateInvocationOfCheckServerTrustedForFirstChainOfTrust() throws CertificateException {
        trustManager.checkServerTrusted(CHAIN_OF_TRUST_1, AUTH_TYPE_1);
        trustManager.checkServerTrusted(CHAIN_OF_TRUST_1, AUTH_TYPE_1, engine);
        trustManager.checkServerTrusted(CHAIN_OF_TRUST_1, AUTH_TYPE_1, socket);

        verify(mainTrustManagerOne).checkServerTrusted(CHAIN_OF_TRUST_1, AUTH_TYPE_1);
        verify(mainTrustManagerOne).checkServerTrusted(CHAIN_OF_TRUST_1, AUTH_TYPE_1, engine);
        verify(mainTrustManagerOne).checkServerTrusted(CHAIN_OF_TRUST_1, AUTH_TYPE_1, socket);
    }

    @Test
    public void shouldDelegateInvocationOfCheckServerTrustedForSecondChainOfTrust() throws CertificateException {
        trustManager.checkServerTrusted(CHAIN_OF_TRUST_2, AUTH_TYPE_2);
        trustManager.checkServerTrusted(CHAIN_OF_TRUST_2, AUTH_TYPE_2, engine);
        trustManager.checkServerTrusted(CHAIN_OF_TRUST_2, AUTH_TYPE_2, socket);

        verify(mainTrustManagerOne).checkServerTrusted(CHAIN_OF_TRUST_2, AUTH_TYPE_2);
        verify(mainTrustManagerOne).checkServerTrusted(CHAIN_OF_TRUST_2, AUTH_TYPE_2, engine);
        verify(mainTrustManagerOne).checkServerTrusted(CHAIN_OF_TRUST_2, AUTH_TYPE_2, socket);
    }

    @Test
    public void shouldThrowAnExceptionWhenServerCertificateIsNotTrusted() throws CertificateException {
        CertificateException exception = new CertificateException("For test purposes");
        doThrow(exception).when(mainTrustManagerOne).checkServerTrusted(CHAIN_OF_TRUST_3, AUTH_TYPE_3);
        doThrow(exception).when(mainTrustManagerOne).checkServerTrusted(CHAIN_OF_TRUST_3, AUTH_TYPE_3, engine);
        doThrow(exception).when(mainTrustManagerOne).checkServerTrusted(CHAIN_OF_TRUST_3, AUTH_TYPE_3, socket);

        assertThatThrown(() -> trustManager.checkServerTrusted(CHAIN_OF_TRUST_3, AUTH_TYPE_3), sameInstance(exception));
        assertThatThrown(() -> trustManager.checkServerTrusted(CHAIN_OF_TRUST_3, AUTH_TYPE_3, engine), sameInstance(exception));
        assertThatThrown(() -> trustManager.checkServerTrusted(CHAIN_OF_TRUST_3, AUTH_TYPE_3, socket), sameInstance(exception));
    }

    @Test
    public void shouldUseVariousTrustManagers() throws CertificateException {
        Supplier<X509ExtendedTrustManager> supplier = Mockito.mock(Supplier.class);
        when(supplier.get()).thenReturn(mainTrustManagerOne, mainTrustManagerTwo, mainTrustManagerThree);
        this.trustManager = new RefreshableX509TrustManager("custom-supplier", supplier);

        trustManager.checkServerTrusted(CHAIN_OF_TRUST_1, AUTH_TYPE_3);
        trustManager.checkServerTrusted(CHAIN_OF_TRUST_2, AUTH_TYPE_2);
        trustManager.checkServerTrusted(CHAIN_OF_TRUST_3, AUTH_TYPE_1);

        verify(mainTrustManagerOne).checkServerTrusted(CHAIN_OF_TRUST_1, AUTH_TYPE_3);
        verify(mainTrustManagerTwo).checkServerTrusted(CHAIN_OF_TRUST_2, AUTH_TYPE_2);
        verify(mainTrustManagerThree).checkServerTrusted(CHAIN_OF_TRUST_3, AUTH_TYPE_1);
    }

    @Test
    public void shouldReturnAcceptedIssuersFirst() {
        when(mainTrustManagerOne.getAcceptedIssuers()).thenReturn(CHAIN_OF_TRUST_1);

        X509Certificate[] acceptedIssuers = trustManager.getAcceptedIssuers();

        assertThat(acceptedIssuers, sameInstance(CHAIN_OF_TRUST_1));
    }

    @Test
    public void shouldReturnAcceptedIssuersSecond() {
        when(mainTrustManagerOne.getAcceptedIssuers()).thenReturn(CHAIN_OF_TRUST_2);

        X509Certificate[] acceptedIssuers = trustManager.getAcceptedIssuers();

        assertThat(acceptedIssuers, sameInstance(CHAIN_OF_TRUST_2));
    }

    @Test
    public void shouldDelegateInvocationOfCheckClientTrustedForFirstChainOfTrust() throws CertificateException {
        trustManager.checkClientTrusted(CHAIN_OF_TRUST_1, AUTH_TYPE_1);
        trustManager.checkClientTrusted(CHAIN_OF_TRUST_1, AUTH_TYPE_1, engine);
        trustManager.checkClientTrusted(CHAIN_OF_TRUST_1, AUTH_TYPE_1, socket);

        verify(mainTrustManagerOne).checkClientTrusted(CHAIN_OF_TRUST_1, AUTH_TYPE_1);
        verify(mainTrustManagerOne).checkClientTrusted(CHAIN_OF_TRUST_1, AUTH_TYPE_1, engine);
        verify(mainTrustManagerOne).checkClientTrusted(CHAIN_OF_TRUST_1, AUTH_TYPE_1, socket);
    }

    @Test
    public void shouldDelegateInvocationOfCheckClientTrustedForSecondChainOfTrust() throws CertificateException {
        trustManager.checkClientTrusted(CHAIN_OF_TRUST_2, AUTH_TYPE_2);
        trustManager.checkClientTrusted(CHAIN_OF_TRUST_2, AUTH_TYPE_2, engine);
        trustManager.checkClientTrusted(CHAIN_OF_TRUST_2, AUTH_TYPE_2, socket);

        verify(mainTrustManagerOne).checkClientTrusted(CHAIN_OF_TRUST_2, AUTH_TYPE_2);
        verify(mainTrustManagerOne).checkClientTrusted(CHAIN_OF_TRUST_2, AUTH_TYPE_2, engine);
        verify(mainTrustManagerOne).checkClientTrusted(CHAIN_OF_TRUST_2, AUTH_TYPE_2, socket);
    }

    @Test
    public void shouldThrowAnExceptionWhenClientCertificateIsNotTrusted() throws CertificateException {
        CertificateException exception = new CertificateException("For test purposes");
        doThrow(exception).when(mainTrustManagerOne).checkClientTrusted(CHAIN_OF_TRUST_3, AUTH_TYPE_3);
        doThrow(exception).when(mainTrustManagerOne).checkClientTrusted(CHAIN_OF_TRUST_3, AUTH_TYPE_3, engine);
        doThrow(exception).when(mainTrustManagerOne).checkClientTrusted(CHAIN_OF_TRUST_3, AUTH_TYPE_3, socket);

        assertThatThrown(() -> trustManager.checkClientTrusted(CHAIN_OF_TRUST_3, AUTH_TYPE_3), sameInstance(exception));
        assertThatThrown(() -> trustManager.checkClientTrusted(CHAIN_OF_TRUST_3, AUTH_TYPE_3, engine), sameInstance(exception));
        assertThatThrown(() -> trustManager.checkClientTrusted(CHAIN_OF_TRUST_3, AUTH_TYPE_3, socket), sameInstance(exception));
    }

}