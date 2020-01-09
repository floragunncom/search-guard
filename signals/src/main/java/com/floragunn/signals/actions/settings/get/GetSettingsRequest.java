package com.floragunn.signals.actions.settings.get;

import java.io.IOException;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class GetSettingsRequest extends ActionRequest {

    private String key;

    public GetSettingsRequest() {
        super();
    }

    public GetSettingsRequest(String key) {
        super();
        this.key = key;
    }

    public GetSettingsRequest(StreamInput in) throws IOException {
        super(in);
        this.key = in.readString();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(key);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

}
