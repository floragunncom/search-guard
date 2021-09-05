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

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.RequestLine;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.apache.http.util.EntityUtils;

public class TestHttpHandler implements HttpRequestHandler {
	public String method;
	public String uri;
	public String body;
	
	@Override
	public void handle(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {
		RequestLine requestLine = request.getRequestLine();
		this.method = requestLine.getMethod();
		this.uri = requestLine.getUri();
		
		HttpEntity entity = null;
		if (request instanceof HttpEntityEnclosingRequest) {
			entity = ((HttpEntityEnclosingRequest) request).getEntity();
			body = EntityUtils.toString(entity, StandardCharsets.UTF_8);
		}
	}
	
	public void reset() {
		this.body = null;
		this.uri = null;
		this.method = null;
	}
}
