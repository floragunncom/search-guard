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
package com.floragunn.signals.actions.settings.get;

import java.io.IOException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

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
