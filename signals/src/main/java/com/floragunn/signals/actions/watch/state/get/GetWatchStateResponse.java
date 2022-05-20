
package com.floragunn.signals.actions.watch.state.get;

import java.io.IOException;
import java.util.Map;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.rest.RestStatus;

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
