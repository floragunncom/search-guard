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
package com.floragunn.signals.actions.settings.put;

import java.io.IOException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class PutSettingsRequest extends ActionRequest {

    private String key;
    private String value;
    private boolean valueJson;

    public PutSettingsRequest() {
        super();
    }

    public PutSettingsRequest(String key, String value, boolean valueJson) {
        super();
        this.key = key;
        this.value = value;
        this.valueJson = valueJson;
    }

    public PutSettingsRequest(StreamInput in) throws IOException {
        super(in);
        this.key = in.readString();
        this.value = in.readOptionalString();
        this.valueJson = in.readBoolean();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(key);
        out.writeOptionalString(value);
        out.writeBoolean(valueJson);
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

    public boolean isValueJson() {
        return valueJson;
    }

    public void setValueJson(boolean valueJson) {
        this.valueJson = valueJson;
    }

    @Override
    public String toString() {
        return "PutSettingsRequest [key=" + key + ", value=" + value + ", valueJson=" + valueJson + "]";
    }

}
