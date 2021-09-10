/*
 * Copyright 2020 floragunn GmbH
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

package com.floragunn.searchsupport.rest;

import java.io.ByteArrayInputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestStatus;

import com.floragunn.codova.documents.DocWriter;
import com.floragunn.codova.validation.ConfigValidationException;
import com.google.common.base.Charsets;

public class Responses {
    private static final Logger log = LogManager.getLogger(Responses.class);

    public static void sendError(RestChannel channel, RestStatus status, String error) {
        sendError(channel, status, error, (String) null);
    }

    public static void sendError(RestChannel channel, RestStatus status, String error, String detailJsonDocument) {

        try {
            final XContentBuilder builder = channel.newBuilder();
            builder.prettyPrint();
            builder.humanReadable(true);
            builder.startObject();
            builder.field("status", status.getStatus());

            if (error != null) {
                builder.field("error", error);
            }

            if (detailJsonDocument != null) {
                builder.rawField("detail", new ByteArrayInputStream(detailJsonDocument.getBytes(Charsets.UTF_8)), XContentType.JSON);
            }
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(status, builder));
        } catch (Exception e) {
            log.error(e.toString(), e);
            throw ExceptionsHelper.convertToOpenSearchException(e);
        }
    }

    public static void sendError(RestChannel channel, RestStatus status, String error, ToXContent detailDocument) {

        try {
            final XContentBuilder builder = channel.newBuilder();
            builder.prettyPrint();
            builder.humanReadable(true);
            builder.startObject();
            builder.field("status", status.getStatus());

            if (error != null) {
                builder.field("error", error);
            }

            if (detailDocument != null) {
                builder.field("detail", detailDocument);
            }
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(status, builder));
        } catch (Exception e) {
            log.error(e.toString(), e);
            throw ExceptionsHelper.convertToOpenSearchException(e);
        }
    }

    public static void sendError(RestChannel channel, Exception e) {
        if (e instanceof ConfigValidationException) {
            sendError(channel, RestStatus.BAD_REQUEST, e.getMessage(), ((ConfigValidationException) e).getValidationErrors().toJsonString());
        } else {
            sendError(channel, ExceptionsHelper.status(e), e.getMessage());
        }
    }

    public static void send(RestChannel channel, RestStatus status) {
        try {
            final XContentBuilder builder = channel.newBuilder();
            builder.prettyPrint();
            builder.humanReadable(true);
            builder.startObject();
            builder.field("status", status.getStatus());

            builder.endObject();
            channel.sendResponse(new BytesRestResponse(status, builder));
        } catch (Exception e) {
            log.error(e.toString(), e);
            throw ExceptionsHelper.convertToOpenSearchException(e);
        }
    }

    public static void sendJson(RestChannel channel, Object json) {
        try {
            if (json instanceof ToXContent) {
                ToXContent toxContent = (ToXContent) json;

                XContentBuilder builder = channel.newBuilder();

                if (toxContent.isFragment()) {
                    builder.startObject();
                }
                toxContent.toXContent(builder, ToXContent.EMPTY_PARAMS);

                if (toxContent.isFragment()) {
                    builder.endObject();
                }
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            } else {
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/json", DocWriter.json().writeAsString(json)));
            }
        } catch (Exception e) {
            log.error(e.toString(), e);
            throw ExceptionsHelper.convertToOpenSearchException(e);
        }
    }
}
