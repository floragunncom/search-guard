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
import org.mockito.junit.MockitoJUnitRunner;

import javax.net.ssl.SSLEngine;
import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import static com.floragunn.searchsupport.junit.ThrowableAssert.assertThatThrown;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.instanceOf;

@RunWith(MockitoJUnitRunner.class)
public class RejectAllTrustManagerTest {
    public static final String AUTH_TYPE = "auth-type";
    @Mock
    private Socket socket;
    @Mock
    private SSLEngine engine;
    @Mock
    private X509Certificate certificate;

    private RejectAllTrustManager trustManager;

    @Before
    public void before() {
        this.trustManager = new RejectAllTrustManager("missing truststore id");
    }

    @Test
    public void shouldNotTrustServerCertificates() {
        assertThatThrown(() -> trustManager.checkServerTrusted(new X509Certificate[]{certificate}, AUTH_TYPE),
            instanceOf(CertificateException.class));
        assertThatThrown(() -> trustManager.checkServerTrusted(new X509Certificate[1], AUTH_TYPE, socket),
            instanceOf(CertificateException.class));
        assertThatThrown(() -> trustManager.checkServerTrusted(new X509Certificate[1], AUTH_TYPE, engine),
            instanceOf(CertificateException.class));
    }

    @Test
    public void shouldNotTrustClientCertificates() {
        assertThatThrown(() -> trustManager.checkClientTrusted(new X509Certificate[]{certificate}, AUTH_TYPE),
            instanceOf(CertificateException.class));
        assertThatThrown(() -> trustManager.checkClientTrusted(new X509Certificate[1], AUTH_TYPE, socket),
            instanceOf(CertificateException.class));
        assertThatThrown(() -> trustManager.checkClientTrusted(new X509Certificate[1], AUTH_TYPE, engine),
            instanceOf(CertificateException.class));
    }

    @Test
    public void shouldReturnEmptyAcceptedIssuer() {
        X509Certificate[] acceptedIssuers = trustManager.getAcceptedIssuers();

        assertThat(acceptedIssuers, arrayWithSize(0));
    }
}