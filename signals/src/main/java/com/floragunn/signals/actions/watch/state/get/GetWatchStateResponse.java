
package com.floragunn.signals.actions.watch.state.get;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

public class GetWatchStateResponse extends ActionResponse implements ToXContentObject {

    private RestStatus restStatus;
    private Map<String, BytesReference> watchToStatusMap;

    public GetWatchStateResponse() {
    }

    public GetWatchStateResponse(RestStatus restStatus, Map<String, BytesReference> watchToStatusMap) {
        this.restStatus = restStatus;
        this.watchToStatusMap = watchToStatusMap;
    }

    public GetWatchStateResponse(StreamInput in) throws IOException {
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

    public RestStatus status() {
        return restStatus;
    }

    public Map<String, BytesReference> getWatchToStatusMap() {
        return watchToStatusMap;
    }

}
