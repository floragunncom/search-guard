package com.floragunn.searchguard.authc.rest;

import com.floragunn.searchguard.authc.RequestMetaData;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.elasticsearch.http.HttpRequest;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class HttpRequestMetadata extends RequestMetaData<HttpRequest> {

    public HttpRequestMetadata(HttpRequest request, ClientAddressAscertainer.ClientIpInfo clientIpInfo, String clientCertSubject) {
        super(request, clientIpInfo, clientCertSubject);
    }

    @Override
    public String getUri() {
        return getRequest().uri();
    }

    @Override
    public String getHeader(String headerName) {
        return getRequest().header(headerName);
    }

    @Override
    public Map<String, List<String>> getHeaders() {
        return getRequest().getHeaders();
    }

    @Override
    public String getParam(String paramName) {
        String uri = getRequest().uri();
        List<NameValuePair> params = URLEncodedUtils.parse(URI.create(uri), StandardCharsets.UTF_8);
        for (NameValuePair param : params) {
            if (param.getName().equals(paramName)) {
                return param.getValue();
            }
        }
        return null;
    }
}
