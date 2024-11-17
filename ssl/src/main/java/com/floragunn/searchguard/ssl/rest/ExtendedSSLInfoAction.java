/*
 * Copyright 2024 floragunn GmbH
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

package com.floragunn.searchguard.ssl.rest;

import static org.elasticsearch.rest.RestRequest.Method.GET;

import java.io.IOException;
import java.security.AlgorithmParameters;
import java.security.KeyFactory;
import java.security.Provider;
import java.security.Provider.Service;
import java.security.Security;
import java.security.Signature;
import java.security.cert.CertificateFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.crypto.Cipher;
import javax.crypto.KeyAgreement;
import javax.crypto.Mac;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import com.google.common.collect.ImmutableList;

public class ExtendedSSLInfoAction extends BaseRestHandler {

    public ExtendedSSLInfoAction() {
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(GET, "/_searchguard/sslinfo/extended"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return new RestChannelConsumer() {
            @Override
            public void accept(RestChannel channel) throws Exception {
                channel.sendResponse(new RestResponse(RestStatus.OK, buildInfo()));
            }
        };
    }

    @Override
    public String getName() {
        return "/_searchguard/sslinfo/extended";
    }

    private String buildInfo() {
        Set<String> cipherNames = new TreeSet<>();
        Set<String> signatureNames = new TreeSet<>();
        Set<String> keyFactoryNames = new TreeSet<>();
        Set<String> macNames = new TreeSet<>();
        Set<String> keyAgreementNames = new TreeSet<>();
        Set<String> certificateFactoryNames = new TreeSet<>();
        Set<String> algorithmParameterNames = new TreeSet<>();

        StringBuilder result = new StringBuilder();

        for (Provider provider : Security.getProviders()) {
            result.append("\n# Provider: ").append(provider.getName()).append("\n\n");

            result.append("\n## Services\n\n");
            Map<String, Set<String>> typeToService = new TreeMap<>();

            for (Service service : provider.getServices()) {
                typeToService.computeIfAbsent(service.getType(), (k) -> new TreeSet<>()).add(service.toString());

                if (service.getType().equals("Cipher")) {
                    cipherNames.add(service.getAlgorithm());
                } else if (service.getType().equals("Signature")) {
                    signatureNames.add(service.getAlgorithm());
                } else if (service.getType().equals("KeyFactory")) {
                    keyFactoryNames.add(service.getAlgorithm());
                } else if (service.getType().equals("Mac")) {
                    macNames.add(service.getAlgorithm());
                } else if (service.getType().equals("KeyAgreement")) {
                    keyAgreementNames.add(service.getAlgorithm());
                } else if (service.getType().equals("CertificateFactory")) {
                    certificateFactoryNames.add(service.getAlgorithm());
                } else if (service.getType().equals("AlgorithmParameters")) {
                    algorithmParameterNames.add(service.getAlgorithm());
                }
            }

            for (String serviceType : typeToService.keySet()) {
                result.append("\n### " + serviceType + "\n\n");

                for (String serviceInfo : typeToService.get(serviceType)) {
                    result.append("- " + serviceInfo);
                }
            }

        }

        result.append("\n# Effective Cipher Providers\n\n");

        for (String cipherName : cipherNames) {

            try {
                Cipher cipher = Cipher.getInstance(cipherName);

                result.append("- " + cipherName + ": " + cipher.getProvider() + "\n");
            } catch (Exception e) {
                result.append("- " + cipherName + ": " + e + "\n");
            }
        }

        result.append("\n# Effective Signature Providers\n\n");

        for (String signatureName : signatureNames) {
            try {
                Signature signature = Signature.getInstance(signatureName);

                result.append("- " + signatureName + ": " + signature.getProvider() + "\n");
            } catch (Exception e) {
                result.append("- " + signatureName + ": " + e + "\n");
            }
        }

        result.append("\n# Effective Key Factory Providers\n\n");

        for (String keyFactoryName : keyFactoryNames) {
            try {
                KeyFactory keyFactory = KeyFactory.getInstance(keyFactoryName);

                result.append("- " + keyFactoryName + ": " + keyFactory.getProvider() + "\n");
            } catch (Exception e) {
                result.append("- " + keyFactoryName + ": " + e + "\n");
            }
        }

        result.append("\n# Effective MAC Providers\n\n");

        for (String macName : macNames) {
            try {
                Mac mac = Mac.getInstance(macName);

                result.append("- " + macName + ": " + mac.getProvider() + "\n");
            } catch (Exception e) {
                result.append("- " + macName + ": " + e + "\n");
            }
        }

        result.append("\n# Effective Key Agreement Providers\n\n");

        for (String keyAgreementName : keyAgreementNames) {
            try {
                KeyAgreement keyAgreement = KeyAgreement.getInstance(keyAgreementName);

                result.append("- " + keyAgreementName + ": " + keyAgreement.getProvider() + "\n");
            } catch (Exception e) {
                result.append("- " + keyAgreementName + ": " + e + "\n");
            }
        }
        
        result.append("\n# Effective Certificate Factory Providers\n\n");

        for (String certificateFactoryName : certificateFactoryNames) {
            try {
                CertificateFactory certificateFactory = CertificateFactory.getInstance(certificateFactoryName);

                result.append("- " + certificateFactoryName + ": " + certificateFactory.getProvider() + "\n");
            } catch (Exception e) {
                result.append("- " + certificateFactoryName + ": " + e + "\n");
            }
        }
        
        result.append("\n# Effective Algorithm Parameter Providers\n\n");

        for (String algorithmParameterName : algorithmParameterNames) {
            try {
                AlgorithmParameters algorithmParameters = AlgorithmParameters.getInstance(algorithmParameterName);

                result.append("- " + algorithmParameterName + ": " + algorithmParameters.getProvider() + "\n");
            } catch (Exception e) {
                result.append("- " + algorithmParameterName + ": " + e + "\n");
            }
        }
        return result.toString();

    }
}
