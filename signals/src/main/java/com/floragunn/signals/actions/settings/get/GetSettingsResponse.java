package com.floragunn.signals.actions.settings.get;

import java.io.IOException;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class GetSettingsResponse extends ActionResponse {

    private String result;
    private String contentType;
    private Status status;

    public GetSettingsResponse() {
    }

    public GetSettingsResponse(Status status, String result, String contentType) {
        super();
        this.status = status;
        this.contentType = contentType;
        this.result = result;
    }

    public GetSettingsResponse(StreamInput in) throws IOException {
        this.status = in.readEnum(Status.class);
        this.result = in.readOptionalString();
        this.contentType = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(status);
        out.writeOptionalString(result);
        out.writeOptionalString(contentType);
    }

    public enum Status {
        OK, NOT_FOUND
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }
}
