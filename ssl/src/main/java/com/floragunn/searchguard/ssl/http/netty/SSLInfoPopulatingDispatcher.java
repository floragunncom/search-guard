/*
 * Copyright 2026 floragunn GmbH
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

package com.floragunn.searchguard.ssl.http.netty;

import com.floragunn.searchguard.ssl.SslExceptionHandler;
import com.floragunn.searchguard.ssl.http.AttributedHttpRequest;
import com.floragunn.searchguard.ssl.transport.PrincipalExtractor;
import com.floragunn.searchguard.ssl.util.ExceptionUtils;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import io.netty.handler.ssl.SslHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static com.floragunn.searchguard.ssl.util.SSLCertificateHelper.validate;

public class SSLInfoPopulatingDispatcher implements HttpServerTransport.Dispatcher {
    private static final Logger log = LogManager.getLogger(SSLInfoPopulatingDispatcher.class);

    private final HttpServerTransport.Dispatcher originalDispatcher;
    private final SslExceptionHandler errorHandler;
    private final PrincipalExtractor principalExtractor;
    private final Settings settings;
    private final Path configPath;

    private final boolean httpSSLEnabled;

    public SSLInfoPopulatingDispatcher(HttpServerTransport.Dispatcher originalDispatcher, SslExceptionHandler errorHandler, PrincipalExtractor principalExtractor, Settings settings, Path configPath) {
        this.originalDispatcher = originalDispatcher;
        this.errorHandler = errorHandler;
        this.principalExtractor = principalExtractor;
        this.settings = settings;
        this.configPath = configPath;

        httpSSLEnabled = settings.getAsBoolean(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_ENABLED,
                SSLConfigConstants.SEARCHGUARD_SSL_HTTP_ENABLED_DEFAULT);
    }

    @Override
    public void dispatchRequest(RestRequest restRequest, RestChannel restChannel, ThreadContext threadContext) {
        if (checkHeaders(restRequest, restChannel, threadContext) && checkAndPopulateSSLInfo(restRequest, restChannel, threadContext)) {
            originalDispatcher.dispatchRequest(restRequest, restChannel, threadContext);
        }
    }

    @Override
    public void dispatchBadRequest(RestChannel restChannel, ThreadContext threadContext, Throwable throwable) {
        if (checkHeaders(restChannel.request(), restChannel, threadContext) && checkAndPopulateSSLInfo(restChannel.request(), restChannel, threadContext)) {
            originalDispatcher.dispatchBadRequest(restChannel, threadContext, throwable);
        }
    }

    private boolean checkHeaders(RestRequest request, RestChannel channel, ThreadContext context) {
        if (!httpSSLEnabled) {
            return true;
        }
        for (final Map.Entry<String, String> header : context.getHeaders().entrySet()) {
            if (header != null && header.getKey() != null && header.getKey().trim().toLowerCase().startsWith(SSLConfigConstants.SG_SSL_PREFIX)) {
                final ElasticsearchException exception = ExceptionUtils.createBadHeaderException();
                log.error(exception);
                errorHandler.logError(exception, request, 1);
                try {
                    channel.sendResponse(new RestResponse(channel, RestStatus.FORBIDDEN, exception));
                } catch (IOException e) {
                    log.error(e,e);
                    channel.sendResponse(new RestResponse(RestStatus.INTERNAL_SERVER_ERROR, RestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
                }
                return false;
            }
        }
        return true;
    }

    private boolean checkAndPopulateSSLInfo(RestRequest request, RestChannel channel, ThreadContext threadContext) {
        final SslHandler sslhandler = Optional.ofNullable(request.getHttpRequest())
                .filter(AttributedHttpRequest.class::isInstance)
                .map(AttributedHttpRequest.class::cast)
                .flatMap(attributedRequest -> Optional.ofNullable(attributedRequest.getSslHandler()))
                .orElse(null);
        if(sslhandler == null) {
            if (httpSSLEnabled) {
                log.error("Not an SSL request");
                throw new ElasticsearchSecurityException("Not an SSL request", RestStatus.INTERNAL_SERVER_ERROR);
            }
            return true;
        }

        final SSLEngine engine = sslhandler.engine();
        final SSLSession session = engine.getSession();

        X509Certificate[] x509Certs = null;
        final String protocol = session.getProtocol();
        final String cipher = session.getCipherSuite();
        final Certificate[] localCerts = session.getLocalCertificates();
        String principal = null;
        boolean validationFailure = false;

        if (engine.getNeedClientAuth() || engine.getWantClientAuth()) {

            try {
                final Certificate[] certs = session.getPeerCertificates();

                if (certs != null && certs.length > 0 && certs[0] instanceof X509Certificate) {
                    x509Certs = Arrays.copyOf(certs, certs.length, X509Certificate[].class);
                    final X509Certificate[] x509CertsF = x509Certs;

                    final SecurityManager sm = System.getSecurityManager();

                    if (sm != null) {
                        sm.checkPermission(new SpecialPermission());
                    }

                    validationFailure = AccessController.doPrivileged((PrivilegedAction<Boolean>) () -> !validate(x509CertsF, settings, configPath));

                    if(validationFailure) {
                        throw new SSLPeerUnverifiedException("Unable to validate certificate (CRL)");
                    }
                    principal = principalExtractor == null?null: principalExtractor.extractPrincipal(x509Certs[0], PrincipalExtractor.Type.HTTP);
                } else if (engine.getNeedClientAuth()) {
                    throw new ElasticsearchException("No client certificates found but such are needed (SG 9).");
                }
            } catch (final SSLPeerUnverifiedException e) {
                if (engine.getNeedClientAuth() || validationFailure) {
                    log.error("No ssl info", e);
                    errorHandler.logError(e, request, 0);
                    try {
                        channel.sendResponse(new RestResponse(channel, RestStatus.FORBIDDEN, e));
                    } catch (IOException ex) {
                        log.error(e,e);
                        channel.sendResponse(new RestResponse(RestStatus.INTERNAL_SERVER_ERROR, RestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
                    }
                    return false;
                }
            }
        }

        if (principal != null) {
            threadContext.putTransient(SSLConfigConstants.SG_SSL_PRINCIPAL, principal);
        }
        if (x509Certs != null) {
            threadContext.putTransient(SSLConfigConstants.SG_SSL_PEER_CERTIFICATES, x509Certs);
        }
        if (localCerts != null) {
            threadContext.putTransient(SSLConfigConstants.SG_SSL_LOCAL_CERTIFICATES, localCerts);
        }
        threadContext.putTransient(SSLConfigConstants.SG_SSL_PROTOCOL, protocol);
        threadContext.putTransient(SSLConfigConstants.SG_SSL_CIPHER, cipher);
        return true;
    }
}
