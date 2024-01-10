package com.floragunn.searchsupport.proxy.wiremock;

import com.github.tomakehurst.wiremock.extension.requestfilter.RequestFilterAction;
import com.github.tomakehurst.wiremock.extension.requestfilter.RequestWrapper;
import com.github.tomakehurst.wiremock.extension.requestfilter.StubRequestFilterV2;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;

import java.util.Objects;

public class WireMockRequestHeaderAddingFilter implements StubRequestFilterV2 {

    private final String headerName;
    private final String headerValue;

    public WireMockRequestHeaderAddingFilter(String headerName, String headerValue) {
        this.headerName = Objects.requireNonNull(headerName, "header name is required");
        this.headerValue = Objects.requireNonNull(headerValue, "header value is required");
    }

    @Override
    public RequestFilterAction filter(Request request, ServeEvent serveEvent) {
        Request modifiedRequest = RequestWrapper.create()
                .addHeader(headerName, headerValue)
                .wrap(request);
        return RequestFilterAction.continueWith(modifiedRequest);
    }

    @Override
    public String getName() {
        return String.format("Add '%s' header to request", headerName);
    }

    public Header getHeader() {
        return new BasicHeader(headerName, headerValue);
    }
}
