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
        super(in);
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
