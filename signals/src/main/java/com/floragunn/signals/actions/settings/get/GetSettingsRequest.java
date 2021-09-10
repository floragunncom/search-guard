package com.floragunn.signals.actions.settings.get;

import java.io.IOException;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

public class GetSettingsRequest extends ActionRequest {

    private String key;
    private boolean jsonRequested;
    
    public GetSettingsRequest() {
        super();
    }

    public GetSettingsRequest(String key, boolean jsonRequested) {
        super();
        this.key = key;
        this.jsonRequested = jsonRequested;
    }

    public GetSettingsRequest(StreamInput in) throws IOException {
        super(in);
        this.key = in.readString();
        this.jsonRequested = in.readBoolean();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(key);
        out.writeBoolean(jsonRequested);
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

    public boolean isJsonRequested() {
        return jsonRequested;
    }

    public void setJsonRequested(boolean jsonRequested) {
        this.jsonRequested = jsonRequested;
    }

}
