package com.floragunn.signals.actions.settings.put;

import java.io.IOException;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class PutSettingsRequest extends ActionRequest {

    private String key;
    private String value;

    public PutSettingsRequest() {
        super();
    }

    public PutSettingsRequest(String key, String value) {
        super();
        this.key = key;
        this.value = value;
    }

    public PutSettingsRequest(StreamInput in) throws IOException {
        super(in);
        this.key = in.readString();
        this.value = in.readOptionalString();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(key);
        out.writeOptionalString(value);
    }

    @Override
    public ActionRequestValidationException validate() {
        if (key == null || key.length() == 0) {
            return new ActionRequestValidationException();
        }
        return null;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

   
}
