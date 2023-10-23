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

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.signals.truststore.rest.TruststoreLoader;
import com.floragunn.signals.truststore.service.TrustManagerRegistry;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509ExtendedTrustManager;
import java.util.Optional;

import static com.floragunn.searchsupport.junit.ThrowableAssert.assertThatThrown;
import static com.floragunn.signals.truststore.rest.TruststoreLoader.PEM_TWO_CERTIFICATES;
import static com.floragunn.signals.watch.common.ValidationLevel.LENIENT;
import static com.floragunn.signals.watch.common.ValidationLevel.STRICT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TlsConfigTest {

    public static final String TRUSTSTORE_ID_1 = "truststore_id_00001";
    public static final String TRUSTSTORE_ID_2 = "truststore_id_00002";
    public static final String TRUSTSTORE_ID_3 = "truststore_id_00003";
    public static final int HALF_HOUR_SECOND = 1800;
    public static final int HOUR_SECOND = 3600;
    @Mock
    private TrustManagerRegistry trustManagerRegistry;
    @Mock
    private X509ExtendedTrustManager x509TrustManager;

    private TlsConfig tlsConfig;

    @Before
    public void before() {
        this.tlsConfig = new TlsConfig(trustManagerRegistry, STRICT);
    }

    @Test
    public void shouldUseTrustManagerWithProvidedId() throws ConfigValidationException {
        when(trustManagerRegistry.findTrustManager(TRUSTSTORE_ID_1)).thenReturn(Optional.of(x509TrustManager));
        tlsConfig.setTruststoreId(TRUSTSTORE_ID_1);

        tlsConfig.init();

        verify(trustManagerRegistry).findTrustManager(TRUSTSTORE_ID_1);
    }

    @Test
    public void shouldUseOtherTrustManager() throws ConfigValidationException {
        when(trustManagerRegistry.findTrustManager(TRUSTSTORE_ID_2)).thenReturn(Optional.of(x509TrustManager));
        tlsConfig.setTruststoreId(TRUSTSTORE_ID_2);

        tlsConfig.init();

        verify(trustManagerRegistry).findTrustManager(TRUSTSTORE_ID_2);
    }

    @Test
    public void shouldReportErrorWhenTruststoreWithGivenIdDoesNotExist() {
        when(trustManagerRegistry.findTrustManager(TRUSTSTORE_ID_3)).thenReturn(Optional.empty());
        tlsConfig.setTruststoreId(TRUSTSTORE_ID_3);

        ConfigValidationException exception =
            (ConfigValidationException) assertThatThrown(() -> tlsConfig.init(), instanceOf(ConfigValidationException.class));

        assertThat(exception.getMessage(), equalTo("Trust store truststore_id_00003 not found."));
    }

    @Test
    public void shouldNotReportErrorWhenTruststoreWithGivenIdDoesNotExistAndStrictValidationIsDisabled() throws ConfigValidationException {
        tlsConfig = new TlsConfig(trustManagerRegistry, LENIENT);
        lenient().when(trustManagerRegistry.findTrustManager(TRUSTSTORE_ID_3)).thenReturn(Optional.empty());
        tlsConfig.setTruststoreId(TRUSTSTORE_ID_3);

        tlsConfig.init();

        // exception is not thrown
    }

    @Test
    public void shouldReadTruststoreIdFromJson() throws ConfigValidationException {
        when(trustManagerRegistry.findTrustManager(TRUSTSTORE_ID_3)).thenReturn(Optional.of(x509TrustManager));

        tlsConfig.init(DocNode.of("truststore_id", TRUSTSTORE_ID_3));

        verify(trustManagerRegistry).findTrustManager(TRUSTSTORE_ID_3);
    }

    @Test
    public void shouldReportValidationErrorWhenTruststoreIsPointedByTwoParameters() {
        String certificate = TruststoreLoader.loadCertificates(PEM_TWO_CERTIFICATES);
        when(trustManagerRegistry.findTrustManager(TRUSTSTORE_ID_1)).thenReturn(Optional.of(x509TrustManager));
        DocNode invalidConfiguration = DocNode.of("truststore_id", TRUSTSTORE_ID_1, "trusted_certs", certificate);

        assertThatThrown(() -> tlsConfig.init(invalidConfiguration), instanceOf(ConfigValidationException.class));
    }

    @Test
    public void shouldSetTlsSessionClientTimeout() throws ConfigValidationException {
        when(trustManagerRegistry.findTrustManager(TRUSTSTORE_ID_1)).thenReturn(Optional.of(x509TrustManager));
        DocNode invalidConfiguration = DocNode.of("truststore_id", TRUSTSTORE_ID_1, "client_session_timeout", HALF_HOUR_SECOND);
        tlsConfig.init(invalidConfiguration);
        ValidationErrors validationErrors = new ValidationErrors();

        SSLContext sslContext = tlsConfig.buildSSLContext(validationErrors);

        int sessionTimeout = sslContext.getClientSessionContext().getSessionTimeout();
        assertThat(sessionTimeout, equalTo(HALF_HOUR_SECOND));
        assertThat(validationErrors.hasErrors(), equalTo(false));
    }

    @Test
    public void shouldSetAnotherTlsSessionClientTimeout() throws ConfigValidationException {
        when(trustManagerRegistry.findTrustManager(TRUSTSTORE_ID_1)).thenReturn(Optional.of(x509TrustManager));
        DocNode invalidConfiguration = DocNode.of("truststore_id", TRUSTSTORE_ID_1, "client_session_timeout", HOUR_SECOND);
        tlsConfig.init(invalidConfiguration);
        ValidationErrors validationErrors = new ValidationErrors();

        SSLContext sslContext = tlsConfig.buildSSLContext(validationErrors);

        int sessionTimeout = sslContext.getClientSessionContext().getSessionTimeout();
        assertThat(sessionTimeout, equalTo(HOUR_SECOND));
        assertThat(validationErrors.hasErrors(), equalTo(false));
    }

}