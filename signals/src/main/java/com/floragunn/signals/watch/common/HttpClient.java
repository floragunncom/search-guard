/*
 * Copyright 2023 floragunn GmbH
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

import java.io.IOException;
import javax.net.ssl.SSLHandshakeException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.HttpContext;

@SuppressWarnings("deprecation")
public class HttpClient extends CloseableHttpClient {

    private final CloseableHttpClient delegate;

    HttpClient(CloseableHttpClient delegate) {
        this.delegate = delegate;
    }

    @Override
    public HttpParams getParams() {
        return delegate.getParams();
    }

    @Override
    public ClientConnectionManager getConnectionManager() {
        return delegate.getConnectionManager();
    }

    @Override
    public void close() throws IOException {
        delegate.close();

    }

    @Override
    protected CloseableHttpResponse doExecute(HttpHost target, HttpRequest request, HttpContext context) throws IOException, ClientProtocolException {
        try {
            return delegate.execute(target, request, context);
        } catch (SSLHandshakeException e) {
            if (e.getMessage().contains("unable to find valid certification path to requested target")) {
                SSLHandshakeException e2 = new SSLHandshakeException(
                        "The server certificate could not be validated using the current truststore: Unable to find valid certification path to requested target. Check authenticity of server and the truststore you are using. ");
                e2.initCause(e);

                throw e2;
            } else if (e.getMessage().contains("Received fatal alert: bad_certificate")) {
                SSLHandshakeException e2 = new SSLHandshakeException(
                        "Certificate validation failed. Check if the host requires client certificate authentication");
                e2.initCause(e);

                throw e2;
            } else {
                throw e;
            }
        } catch (IOException e) {
            throw e;
        }
    }

}
