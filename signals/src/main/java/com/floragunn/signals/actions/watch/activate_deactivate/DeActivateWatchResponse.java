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
package com.floragunn.signals.actions.watch.activate_deactivate;

import java.io.IOException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

public class DeActivateWatchResponse extends ActionResponse implements ToXContentObject {

    private String id;
    private long version;
    private Result result;
    private RestStatus restStatus;
    private String message;

    public DeActivateWatchResponse() {
    }

    public DeActivateWatchResponse(String id, long version, Result result, RestStatus restStatus, String message) {
        this.id = id;
        this.version = version;
        this.result = result;
        this.restStatus = restStatus;
        this.message = message;
    }

    public DeActivateWatchResponse(StreamInput in) throws IOException {
        super(in);
        id = in.readString();
        version = in.readVLong();
        result = in.readEnum(Result.class);
        restStatus = in.readEnum(RestStatus.class);
        message = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeVLong(version);
        out.writeEnum(this.result);
        out.writeEnum(restStatus);
        out.writeOptionalString(message);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("_id", id);
        builder.field("_version", version);
        builder.field("result", result.getLowercase());

        if (message != null) {
            builder.field("message", message);
        }

        builder.endObject();
        return builder;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public Result getResult() {
        return result;
    }

    public void setResult(Result result) {
        this.result = result;
    }

    public RestStatus getRestStatus() {
        return restStatus;
    }

    public void setRestStatus(RestStatus restStatus) {
        this.restStatus = restStatus;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

}
