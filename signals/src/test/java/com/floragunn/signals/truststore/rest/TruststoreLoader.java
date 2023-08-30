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
package com.floragunn.signals.truststore.rest;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class TruststoreLoader {

    private static final Logger log = LogManager.getLogger(TruststoreLoader.class);

    public static final String PEM_THREE_CERTIFICATES = "certs/threeCerts.pem";
    public static final String PEM_TWO_CERTIFICATES = "certs/twoCerts.pem";
    public static final String PEM_ONE_CERTIFICATES = "certs/oneCert.pem";
    public static final String CERT_1_ISSUER = "CN=root.ca.number-0.com,OU=SearchGuard,O=index-0";
    public static final String CERT_1_SUBJECT = "CN=root.ca.number-0.com,OU=SearchGuard,O=index-0";
    public static final String CERT_2_ISSUER = "CN=root.ca.number-1.com,OU=SearchGuard,O=index-1";
    public static final String CERT_2_SUBJECT = "CN=root.ca.number-1.com,OU=SearchGuard,O=index-1";
    public static final String CERT_3_ISSUER = "CN=root.ca.number-2.com,OU=SearchGuard,O=index-2";
    public static final String CERT_3_SUBJECT = "CN=root.ca.number-2.com,OU=SearchGuard,O=index-2";

    public static final String CERT_4_ISSUER = CERT_1_ISSUER;
    public static final String CERT_4_SUBJECT = CERT_1_SUBJECT;

    public static final String NAME_TRUST_STORE = "my PKI";

    public static String loadCertificates(String resourcePath) {
        try(InputStream inputStream = TruststoreLoader.class.getClassLoader().getResourceAsStream(resourcePath)) {
            return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Cannot load resource " + resourcePath, e);
        }
    }

    public static String storeTruststore(GenericRestClient client, String truststoreId, String certificatePath) throws Exception {
        return storeTruststore(client, truststoreId, certificatePath, NAME_TRUST_STORE);
    }

    public static String storeTruststore(GenericRestClient client, String truststoreId, String certificatePath, String nameOfTruststore)
        throws Exception {
        String pem = TruststoreLoader.loadCertificates(certificatePath);
        return storeTruststoreInPemFormat(client, truststoreId, nameOfTruststore, pem);
    }

    public static String storeTruststoreInPemFormat(GenericRestClient client, String truststoreId, String nameOfTruststore, String pem)
        throws Exception {
        String requestBodyString = DocNode.of("name", nameOfTruststore, "pem", pem).toJsonString();
        HttpResponse response = client.putJson("/_signals/truststores/" + truststoreId, requestBodyString);
        log.info("Trust store '{}' upload response contain status code '{}' and body '{}'.", truststoreId, response.getStatusCode(),
            response.getBody());
        assertThat(response.getStatusCode(), equalTo(200));
        return pem;
    }

    public static int deleteTruststoreById(GenericRestClient client, String truststoreId) throws Exception {
        HttpResponse response = client.delete("/_signals/truststores/" + truststoreId);
        return response.getStatusCode();
    }
}
