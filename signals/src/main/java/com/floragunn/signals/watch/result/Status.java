package com.floragunn.signals.watch.result;

import java.io.IOException;

import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.fasterxml.jackson.databind.JsonNode;

public class Status implements ToXContentObject {
    private Status.Code code;
    private String detail;

    public Status(Code code, String detail) {
        this.code = code;
        this.detail = detail;
    }

    public Status.Code getCode() {
        return code;
    }

    public void setCode(Status.Code code) {
        this.code = code;
    }

    public String getDetail() {
        return detail;
    }

    public void setDetail(String detail) {
        this.detail = detail;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.startObject();
        builder.field("code", code);
        builder.field("detail", detail);
        builder.endObject();

        return builder;
    }

    @Override
    public String toString() {
        if (detail == null) {
            return String.valueOf(code);
        } else {
            return code + " " + detail;
        }
    }

    public static Status parse(JsonNode jsonNode) {
        Code code = null;
        String detail = null;

        if (jsonNode.hasNonNull("code")) {
            code = Code.valueOf(jsonNode.get("code").asText());
        }

        if (jsonNode.hasNonNull("detail")) {
            detail = jsonNode.get("detail").asText();
        }

        return new Status(code, detail);
    }

    public static enum Code {
        EXECUTION_FAILED, NO_ACTION, ACTION_EXECUTED, SIMULATED_ACTION_EXECUTED, ACTION_FAILED, ACTION_THROTTLED, ACKED
    }
}