package com.floragunn.signals.watch.common;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchsupport.jobs.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.jobs.config.validation.ValidatingJsonNode;

public class HttpClientConfig extends WatchElement {
    private final Integer connectionTimeoutSecs;
    private final Integer readTimeoutSecs;
    private final TlsConfig tlsConfig;

    public HttpClientConfig(Integer connectionTimeoutSecs, Integer readTimeoutSecs, TlsConfig tlsConfig) {
        this.connectionTimeoutSecs = connectionTimeoutSecs;
        this.readTimeoutSecs = readTimeoutSecs;
        this.tlsConfig = tlsConfig;
    }

    public HttpClient createHttpClient() {

        RequestConfig.Builder configBuilder = RequestConfig.custom();

        if (connectionTimeoutSecs != null) {
            configBuilder.setConnectTimeout(connectionTimeoutSecs * 1000);
            configBuilder.setConnectionRequestTimeout(connectionTimeoutSecs * 1000);
        } else {
            configBuilder.setConnectTimeout(10000);
            configBuilder.setConnectionRequestTimeout(10000);
        }

        if (readTimeoutSecs != null) {
            configBuilder.setSocketTimeout(readTimeoutSecs * 1000);
        } else {
            configBuilder.setSocketTimeout(10000);
        }

        RequestConfig config = configBuilder.build();

        HttpClientBuilder clientBuilder = HttpClients.custom().setDefaultRequestConfig(config);

        clientBuilder.useSystemProperties();
        
        // If no password is set, don't ask other components in the system for credentials
        clientBuilder.setDefaultCredentialsProvider(null);

        if (tlsConfig != null) {
            clientBuilder.setSSLSocketFactory(tlsConfig.toSSLConnectionSocketFactory());
        }

        try {
            SecurityManager sm = System.getSecurityManager();

            if (sm != null) {
                sm.checkPermission(new SpecialPermission());
            }

            return AccessController.doPrivileged((PrivilegedExceptionAction<HttpClient>) () -> new HttpClient(clientBuilder.build()));
        } catch (PrivilegedActionException e) {
            throw new RuntimeException(e.getCause());
        }

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (readTimeoutSecs != null) {
            builder.field("read_timeout", readTimeoutSecs);
        }

        if (connectionTimeoutSecs != null) {
            builder.field("connection_timeout", connectionTimeoutSecs);
        }

        if (tlsConfig != null) {
            builder.field("tls", tlsConfig);
        }

        return builder;
    }

    public static HttpClientConfig create(ValidatingJsonNode jsonObject) throws ConfigValidationException {

        Integer connectionTimeout = null;
        Integer readTimeout = null;
        TlsConfig tlsConfig = null;
        
        // TODO support units

        if (jsonObject.hasNonNull("read_timeout")) {
            readTimeout = jsonObject.get("read_timeout").intValue();
        }

        if (jsonObject.hasNonNull("connection_timeout")) {
            connectionTimeout = jsonObject.get("connection_timeout").intValue();
        }

        JsonNode tlsJsonNode = jsonObject.get("tls");

        if (tlsJsonNode != null) {
            tlsConfig = TlsConfig.create(tlsJsonNode);
        }

        return new HttpClientConfig(connectionTimeout, readTimeout, tlsConfig);

    }
}
