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
