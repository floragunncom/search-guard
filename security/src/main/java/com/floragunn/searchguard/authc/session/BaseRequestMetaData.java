package com.floragunn.searchguard.authc.session;

import org.elasticsearch.rest.RestRequest;

/**
 * HTTP Request uri and headers
 */
public interface BaseRequestMetaData {
    String getUri();

    String getHeader(String headerName);

    static BaseRequestMetaData adapt(RestRequest request) {
        return new BaseRequestMetaData() {
            @Override
            public String getUri() {
                return request.uri();
            }

            @Override
            public String getHeader(String headerName) {
                return request.header(headerName);
            }
        };
    }
}
