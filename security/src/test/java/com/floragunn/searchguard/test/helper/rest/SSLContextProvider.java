package com.floragunn.searchguard.test.helper.rest;

import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;

import javax.net.ssl.SSLContext;

public interface SSLContextProvider {

    SSLContext getSslContext();

    default SSLIOSessionStrategy getSSLIOSessionStrategy() {
        return new SSLIOSessionStrategy(getSslContext(), null, null, NoopHostnameVerifier.INSTANCE);
    }
}
