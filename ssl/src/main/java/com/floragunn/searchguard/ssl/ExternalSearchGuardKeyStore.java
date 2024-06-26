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

package com.floragunn.searchguard.ssl;

import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;

import com.floragunn.searchguard.ssl.util.SSLConfigConstants;

public class ExternalSearchGuardKeyStore implements SearchGuardKeyStore {

    private static final String EXTERNAL = "EXTERNAL";
    private static final Map<String, SSLContext> contextMap = new ConcurrentHashMap<String, SSLContext>();
    private final SSLContext externalSslContext;
    private final Settings settings;

    public ExternalSearchGuardKeyStore(final Settings settings) {
        this.settings = Objects.requireNonNull(settings);
        final String externalContextId = settings
                .get(SSLConfigConstants.SEARCHGUARD_SSL_CLIENT_EXTERNAL_CONTEXT_ID, null);
                
        if(externalContextId == null || externalContextId.length() == 0) {
            throw new ElasticsearchException("no external ssl context id was set");
        }
        
        externalSslContext = contextMap.get(externalContextId);
        
        if(externalSslContext == null) {
            throw new ElasticsearchException("no external ssl context for id "+externalContextId);
        }
    }

    @Override
    public SSLEngine createHTTPSSLEngine() throws SSLException {
        throw new SSLException("not implemented");
    }

    @Override
    public SSLEngine createServerTransportSSLEngine() throws SSLException {
        throw new SSLException("not implemented");
    }

    @Override
    public SSLEngine createClientTransportSSLEngine(final String peerHost, final int peerPort) throws SSLException {
        if (peerHost != null) {
            final SSLEngine engine = externalSslContext.createSSLEngine(peerHost, peerPort);            
            final SSLParameters sslParams = new SSLParameters();
            sslParams.setEndpointIdentificationAlgorithm("HTTPS");
            engine.setSSLParameters(sslParams);
            engine.setEnabledProtocols(evalSecure(engine.getEnabledProtocols(), SSLConfigConstants.getSecureSSLProtocols(settings, false)));
            engine.setEnabledCipherSuites(evalSecure(engine.getEnabledCipherSuites(), SSLConfigConstants.getSecureSSLCiphers(settings, false).toArray(new String[0])));
            engine.setUseClientMode(true);
            return engine;
        } else {
            final SSLEngine engine = externalSslContext.createSSLEngine();
            engine.setEnabledProtocols(evalSecure(engine.getEnabledProtocols(), SSLConfigConstants.getSecureSSLProtocols(settings, false)));
            engine.setEnabledCipherSuites(evalSecure(engine.getEnabledCipherSuites(), SSLConfigConstants.getSecureSSLCiphers(settings, false).toArray(new String[0])));
            engine.setUseClientMode(true);
            return engine;
        }
    }

    @Override
    public String getHTTPProviderName() {
        return null;
    }

    @Override
    public String getTransportServerProviderName() {
        return null;
    }

    @Override
    public String getTransportClientProviderName() {
        return EXTERNAL;
    }

    @Override
    public void initHttpSSLConfig() {
        // NOOP
    }

    @Override
    public void initTransportSSLConfig() {
        // NOOP
    }

    @Override
    public X509Certificate[] getHttpCerts() {
        return new X509Certificate[0];
    }

    @Override
    public X509Certificate[] getTransportCerts() {
        return new X509Certificate[0];
    }

    public static void registerExternalSslContext(String id, SSLContext externalSsslContext) {
        contextMap.put(Objects.requireNonNull(id), Objects.requireNonNull(externalSsslContext));
    }
    
    public static boolean hasExternalSslContext(Settings settings) {
        
        final String externalContextId = settings
                .get(SSLConfigConstants.SEARCHGUARD_SSL_CLIENT_EXTERNAL_CONTEXT_ID, null);
                
        if(externalContextId == null || externalContextId.length() == 0) {
            return false;
        }
        
        return contextMap.containsKey(externalContextId);
    }
    
    public static boolean hasExternalSslContext(String id) {
        return contextMap.containsKey(id);
    }
    
    public static void removeExternalSslContext(String id) {
        contextMap.remove(id);
    }
    
    public static void removeAllExternalSslContexts() {
        contextMap.clear();
    }
    
    private String[] evalSecure(String[] engineEnabled, String[] secure) {
        List<String> tmp = new ArrayList<>(Arrays.asList(engineEnabled));
        tmp.retainAll(Arrays.asList(secure));
        return tmp.toArray(new String[0]);
    }

}
