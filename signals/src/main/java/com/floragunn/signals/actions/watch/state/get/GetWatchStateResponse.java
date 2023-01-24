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
package com.floragunn.signals.actions.watch.state.get;

import java.io.IOException;
import java.util.Map;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

public class GetWatchStateResponse extends ActionResponse implements StatusToXContentObject {

    private RestStatus restStatus;
    private Map<String, BytesReference> watchToStatusMap;

    public GetWatchStateResponse() {
    }

    public GetWatchStateResponse(RestStatus restStatus, Map<String, BytesReference> watchToStatusMap) {
        this.restStatus = restStatus;
        this.watchToStatusMap = watchToStatusMap;
    }

    public GetWatchStateResponse(StreamInput in) throws IOException {
        super(in);
        restStatus = in.readEnum(RestStatus.class);
        watchToStatusMap = in.readMap(StreamInput::readString, StreamInput::readBytesReference);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(restStatus);
        out.writeMap(watchToStatusMap, StreamOutput::writeString, StreamOutput::writeBytesReference);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (Map.Entry<String, BytesReference> entry : watchToStatusMap.entrySet()) {
            builder.rawField(entry.getKey(), entry.getValue().streamInput(), XContentType.JSON);
        }

        builder.endObject();
        return builder;
    }

    @Override
    public RestStatus status() {
        return restStatus;
    }

    public Map<String, BytesReference> getWatchToStatusMap() {
        return watchToStatusMap;
    }

}
