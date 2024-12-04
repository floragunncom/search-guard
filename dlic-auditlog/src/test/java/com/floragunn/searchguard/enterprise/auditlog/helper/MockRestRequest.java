/*
 * Copyright 2016-2017 by floragunn GmbH - All rights reserved
 * 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 
 * This software is free of charge for non-commercial and academic use. 
 * For commercial use in a production environment you have to obtain a license 
 * from https://floragunn.com
 * 
 */

package com.floragunn.searchguard.enterprise.auditlog.helper;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.http.HttpBody;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentParserConfiguration;

public class MockRestRequest extends RestRequest {

    public MockRestRequest() {
        //NamedXContentRegistry xContentRegistry, Map<String, String> params, String path,
        //Map<String, List<String>> headers, HttpRequest httpRequest, HttpChannel httpChannel
        super(XContentParserConfiguration.EMPTY, Collections.emptyMap(), "", Collections.emptyMap(), new HttpRequest() {
            @Override
            public Method method() {
                return Method.GET;
            }

            @Override
            public String uri() {
                return "";
            }

            @Override
            public Map<String, List<String>> getHeaders() {
                return Collections.emptyMap();
            }

            @Override
            public HttpBody body() {
                return HttpBody.empty();
            }

            @Override
            public List<String> strictCookies() {
                return Collections.emptyList();
            }

            @Override
            public HttpVersion protocolVersion() {
                return HttpVersion.HTTP_1_0;
            }

            @Override
            public HttpRequest removeHeader(String header) {
                return this;
            }

            @Override
            public HttpResponse createResponse(RestStatus status, BytesReference content) {
                return null;
            }

            @Override
            public HttpResponse createResponse(RestStatus status, ChunkedRestResponseBodyPart content) {
                return null;
            }

            @Override
            public Exception getInboundException() {
                return null;
            }

            @Override
            public void release() {

            }

            @Override
            public HttpRequest releaseAndCopy() {
                return null;
            }
        }, null);
    }

    @Override
    public Method method() {
        return Method.GET;
    }

    @Override
    public String uri() {
        return "";
    }

    @Override
    public boolean hasContent() {
        return false;
    }

    @Override
    public BytesReference content() {
        return null;
    }
}