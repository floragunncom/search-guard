/*
 * Copyright 2023 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.floragunn.signals.watch.common;

import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HttpUtils {
    private static final Logger log = LogManager.getLogger(HttpUtils.class);

    public static String getEntityAsString(HttpResponse response) throws IllegalCharsetNameException, UnsupportedCharsetException, IOException {
        if (response == null || response.getEntity() == null) {
            return null;
        }

        return CharStreams.toString(new InputStreamReader(response.getEntity().getContent(), getContentEncoding(response.getEntity())));
    }

    public static String getEntityAsDebugString(HttpResponse response) {
        try {
            return CharStreams
                    .toString(new InputStreamReader(response.getEntity().getContent(), getContentEncodingWithFallback(response.getEntity())));
        } catch (Exception e) {
            log.info("Error while decoding " + response, e);
            return e.toString();
        }
    }

    public static String getEntityAsDebugString(HttpEntityEnclosingRequestBase response) {
        try {
            return CharStreams
                    .toString(new InputStreamReader(response.getEntity().getContent(), getContentEncodingWithFallback(response.getEntity())));
        } catch (Exception e) {
            log.info("Error while decoding " + response, e);
            return e.toString();
        }
    }

    public static Charset getContentEncoding(HttpEntity entity) throws IllegalCharsetNameException, UnsupportedCharsetException {
        return entity.getContentEncoding() != null ? Charset.forName(entity.getContentEncoding().getValue()) : Charset.forName("utf-8");

    }

    public static Charset getContentEncodingWithFallback(HttpEntity entity) throws IllegalCharsetNameException, UnsupportedCharsetException {
        try {
            return entity.getContentEncoding() != null ? Charset.forName(entity.getContentEncoding().getValue()) : Charset.forName("utf-8");
        } catch (IllegalCharsetNameException | UnsupportedCharsetException e) {
            log.info("Count not decode charset " + entity.getContentEncoding().getValue(), e);
            return Charset.forName("ascii");
        }
    }

    public static String getRequestAsDebugString(HttpUriRequest request) {
        if (request instanceof HttpEntityEnclosingRequestBase && ((HttpEntityEnclosingRequestBase) request).getEntity() != null) {
            return request + "\n" + HttpUtils.getEntityAsDebugString((HttpEntityEnclosingRequestBase) request);
        } else {
            return request.toString();
        }
    }
}
