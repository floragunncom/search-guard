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

package com.floragunn.searchguard.ssl.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;

public final class SSLConfigConstants {

    public static final String SEARCHGUARD_ALLOW_UNSAFE_DEMOCERTIFICATES = "searchguard.allow_unsafe_democertificates";
    public static final String SEARCHGUARD_SSL_HTTP_ENABLED = "searchguard.ssl.http.enabled";
    public static final boolean SEARCHGUARD_SSL_HTTP_ENABLED_DEFAULT = false;
    public static final String SEARCHGUARD_SSL_HTTP_CLIENTAUTH_MODE = "searchguard.ssl.http.clientauth_mode";
    public static final String SEARCHGUARD_SSL_HTTP_KEYSTORE_ALIAS = "searchguard.ssl.http.keystore_alias";
    public static final String SEARCHGUARD_SSL_HTTP_KEYSTORE_FILEPATH = "searchguard.ssl.http.keystore_filepath";
    public static final String SEARCHGUARD_SSL_HTTP_PEMKEY_FILEPATH = "searchguard.ssl.http.pemkey_filepath";
    public static final String SEARCHGUARD_SSL_HTTP_PEMKEY_PASSWORD = "searchguard.ssl.http.pemkey_password";
    public static final String SEARCHGUARD_SSL_HTTP_PEMCERT_FILEPATH = "searchguard.ssl.http.pemcert_filepath";
    public static final String SEARCHGUARD_SSL_HTTP_PEMTRUSTEDCAS_FILEPATH = "searchguard.ssl.http.pemtrustedcas_filepath";
    public static final String SEARCHGUARD_SSL_HTTP_KEYSTORE_PASSWORD = "searchguard.ssl.http.keystore_password";
    public static final String SEARCHGUARD_SSL_HTTP_KEYSTORE_KEYPASSWORD = "searchguard.ssl.http.keystore_keypassword";
    public static final String SEARCHGUARD_SSL_HTTP_KEYSTORE_TYPE = "searchguard.ssl.http.keystore_type";
    public static final String SEARCHGUARD_SSL_HTTP_TRUSTSTORE_ALIAS = "searchguard.ssl.http.truststore_alias";
    public static final String SEARCHGUARD_SSL_HTTP_TRUSTSTORE_FILEPATH = "searchguard.ssl.http.truststore_filepath";
    public static final String SEARCHGUARD_SSL_HTTP_TRUSTSTORE_PASSWORD = "searchguard.ssl.http.truststore_password";
    public static final String SEARCHGUARD_SSL_HTTP_TRUSTSTORE_TYPE = "searchguard.ssl.http.truststore_type";
    public static final String SEARCHGUARD_SSL_TRANSPORT_ENABLED = "searchguard.ssl.transport.enabled";
    public static final boolean SEARCHGUARD_SSL_TRANSPORT_ENABLED_DEFAULT = true;
    public static final String SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION = "searchguard.ssl.transport.enforce_hostname_verification";
    public static final String SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION_RESOLVE_HOST_NAME = "searchguard.ssl.transport.resolve_hostname";
    public static final String SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_ALIAS = "searchguard.ssl.transport.keystore_alias";
    public static final String SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_FILEPATH = "searchguard.ssl.transport.keystore_filepath";
    public static final String SEARCHGUARD_SSL_TRANSPORT_PEMKEY_FILEPATH = "searchguard.ssl.transport.pemkey_filepath";
    public static final String SEARCHGUARD_SSL_TRANSPORT_PEMKEY_PASSWORD = "searchguard.ssl.transport.pemkey_password";
    public static final String SEARCHGUARD_SSL_TRANSPORT_PEMCERT_FILEPATH = "searchguard.ssl.transport.pemcert_filepath";
    public static final String SEARCHGUARD_SSL_TRANSPORT_PEMTRUSTEDCAS_FILEPATH = "searchguard.ssl.transport.pemtrustedcas_filepath";
    public static final String SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_PASSWORD = "searchguard.ssl.transport.keystore_password";
    public static final String SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_KEYPASSWORD = "searchguard.ssl.transport.keystore_keypassword";
    public static final String SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_TYPE = "searchguard.ssl.transport.keystore_type";
    public static final String SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_ALIAS = "searchguard.ssl.transport.truststore_alias";
    public static final String SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_FILEPATH = "searchguard.ssl.transport.truststore_filepath";
    public static final String SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_PASSWORD = "searchguard.ssl.transport.truststore_password";
    public static final String SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_TYPE = "searchguard.ssl.transport.truststore_type";
    public static final String SEARCHGUARD_SSL_TRANSPORT_ENABLED_CIPHERS = "searchguard.ssl.transport.enabled_ciphers";
    public static final String SEARCHGUARD_SSL_TRANSPORT_ENABLED_PROTOCOLS = "searchguard.ssl.transport.enabled_protocols";
    public static final String SEARCHGUARD_SSL_HTTP_ENABLED_CIPHERS = "searchguard.ssl.http.enabled_ciphers";
    public static final String SEARCHGUARD_SSL_HTTP_ENABLED_PROTOCOLS = "searchguard.ssl.http.enabled_protocols";
    public static final String SEARCHGUARD_SSL_CLIENT_EXTERNAL_CONTEXT_ID = "searchguard.ssl.client.external_context_id";
    public static final String SEARCHGUARD_SSL_TRANSPORT_PRINCIPAL_EXTRACTOR_CLASS = "searchguard.ssl.transport.principal_extractor_class";

    public static final String SEARCHGUARD_SSL_HTTP_CRL_FILE = "searchguard.ssl.http.crl.file_path";
    public static final String SEARCHGUARD_SSL_HTTP_CRL_VALIDATE = "searchguard.ssl.http.crl.validate";
    public static final String SEARCHGUARD_SSL_HTTP_CRL_PREFER_CRLFILE_OVER_OCSP = "searchguard.ssl.http.crl.prefer_crlfile_over_ocsp";
    public static final String SEARCHGUARD_SSL_HTTP_CRL_CHECK_ONLY_END_ENTITIES = "searchguard.ssl.http.crl.check_only_end_entities";    
    public static final String SEARCHGUARD_SSL_HTTP_CRL_DISABLE_CRLDP = "searchguard.ssl.http.crl.disable_crldp";   
    public static final String SEARCHGUARD_SSL_HTTP_CRL_VALIDATION_DATE = "searchguard.ssl.http.crl.validation_date";

    public static final String SEARCHGUARD_SSL_ALLOW_CLIENT_INITIATED_RENEGOTIATION = "searchguard.ssl.allow_client_initiated_renegotiation";

    public static final String DEFAULT_STORE_PASSWORD = "changeit"; //#16
    
    public static final String JDK_TLS_REJECT_CLIENT_INITIATED_RENEGOTIATION = "jdk.tls.rejectClientInitiatedRenegotiation";
    
    private static final String[] _SECURE_SSL_PROTOCOLS = {"TLSv1.3", "TLSv1.2"};

    public static final String[] getSecureSSLProtocols(Settings settings, boolean http)
    {
        List<String> configuredProtocols = null;
        
        if(settings != null) {
            if(http) {
                configuredProtocols = settings.getAsList(SEARCHGUARD_SSL_HTTP_ENABLED_PROTOCOLS, Collections.emptyList());
            } else {
                configuredProtocols = settings.getAsList(SEARCHGUARD_SSL_TRANSPORT_ENABLED_PROTOCOLS, Collections.emptyList());
            }
        }
        
        if(configuredProtocols != null && configuredProtocols.size() > 0) {
            validateIfConfiguredProtocolsAreSecure(configuredProtocols);
            return configuredProtocols.toArray(new String[0]);
        }
        
        return _SECURE_SSL_PROTOCOLS.clone();
    }

    private static void validateIfConfiguredProtocolsAreSecure(List<String> configuredProtocols) {
        Predicate<String> unsecureProtocolFilter = protocol -> Stream.of(_SECURE_SSL_PROTOCOLS)
                .noneMatch(secureProtocol -> secureProtocol.equalsIgnoreCase(protocol));
        String configuredUnsecureProtocols = configuredProtocols.stream()
                .filter(unsecureProtocolFilter)
                .collect(Collectors.joining(", "));

        if (! configuredUnsecureProtocols.isEmpty()) {
            throw new ElasticsearchException(String.format("Protocols: [%s] can not be used since they are outdated and unsecure", configuredUnsecureProtocols));
        }
    }

    // @formatter:off
    private static final String[] _SECURE_SSL_CIPHERS = 
        {
        //TLS_<key exchange and authentication algorithms>_WITH_<bulk cipher and message authentication algorithms>
        
        //Example (including unsafe ones)
        //Protocol: TLS, SSL
        //Key Exchange    RSA, Diffie-Hellman, ECDH, SRP, PSK
        //Authentication  RSA, DSA, ECDSA
        //Bulk Ciphers    RC4, 3DES, AES
        //Message Authentication  HMAC-SHA256, HMAC-SHA1, HMAC-MD5

        //Mozilla modern browsers
        //https://wiki.mozilla.org/Security/Server_Side_TLS
        
        //TLS 1.3
        "TLS_AES_128_GCM_SHA256",
        "TLS_AES_256_GCM_SHA384",
        "TLS_CHACHA20_POLY1305_SHA256", //Open SSL >= 1.1.1 and Java >= 12

        //TLS 1.2 CHACHA20 POLY1305 supported by Java >= 12 and
        //OpenSSL >= 1.1.0
        "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
        "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",
        "TLS_DHE_RSA_WITH_CHACHA20_POLY1305_SHA256",

        "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
        "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
        "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
        "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256",
        "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384"





};
    // @formatter:on
    
    public static final List<String> getSecureSSLCiphers(Settings settings, boolean http) {
        
        List<String> configuredCiphers = null;
        
        if(settings != null) {
            if(http) {
                configuredCiphers = settings.getAsList(SEARCHGUARD_SSL_HTTP_ENABLED_CIPHERS, Collections.emptyList());
            } else {
                configuredCiphers = settings.getAsList(SEARCHGUARD_SSL_TRANSPORT_ENABLED_CIPHERS, Collections.emptyList());
            }
        }
        
        if(configuredCiphers != null && configuredCiphers.size() > 0) {
            return configuredCiphers;
        }

        return Collections.unmodifiableList(Arrays.asList(_SECURE_SSL_CIPHERS));
    }
    
    private SSLConfigConstants() {

    }

}
