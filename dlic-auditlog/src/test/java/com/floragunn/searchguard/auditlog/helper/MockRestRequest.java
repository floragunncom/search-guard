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

package com.floragunn.searchguard.auditlog.helper;

import java.util.Collections;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.rest.RestRequest;

public class MockRestRequest extends RestRequest {

    public MockRestRequest() {
        //NamedXContentRegistry xContentRegistry, Map<String, String> params, String path,
        //Map<String, List<String>> headers, HttpRequest httpRequest, HttpChannel httpChannel
        super(NamedXContentRegistry.EMPTY, Collections.emptyMap(), "", Collections.emptyMap(), null, null);
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