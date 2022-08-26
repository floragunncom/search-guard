package com.floragunn.signals.api;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import com.google.common.base.Charsets;

public abstract class SignalsBaseRestHandler extends BaseRestHandler {
    private static final Logger log = LogManager.getLogger(SignalsBaseRestHandler.class);

    protected SignalsBaseRestHandler(Settings settings) {
        super();
    }

    protected void errorResponse(RestChannel channel, RestStatus status, String error) {
        this.errorResponse(channel, status, error, null);
    }

    protected void errorResponse(RestChannel channel, RestStatus status, String error, String detailJsonDocument) {

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
            channel.sendResponse(new RestResponse(status, builder));
        } catch (Exception e) {
            log.error(e.toString(), e);
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    protected void errorResponse(RestChannel channel, Exception e) {
        RestStatus status = ExceptionsHelper.status(e);
        errorResponse(channel, status, e.getMessage());
    }

    protected void response(RestChannel channel, RestStatus status) {
        try {
            final XContentBuilder builder = channel.newBuilder();
            builder.prettyPrint();
            builder.humanReadable(true);
            builder.startObject();
            builder.field("status", status.getStatus());

            builder.endObject();
            channel.sendResponse(new RestResponse(status, builder));
        } catch (Exception e) {
            log.error(e.toString(), e);
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    protected static XContentBuilder convertToJson(RestChannel channel, ToXContent toXContent, ToXContent.Params params) {
        try {
            XContentBuilder builder = channel.newBuilder();
            builder.prettyPrint();
            toXContent.toXContent(builder, params);
            return builder;
        } catch (IOException e) {
            log.error(e.toString(), e);
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

}
