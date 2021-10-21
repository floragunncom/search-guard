package com.floragunn.searchguard.test.helper.rest;

import javax.net.ssl.SSLContext;

public interface SSLContextProvider {

    SSLContext getSslContext(boolean clientAuthentication);
}
