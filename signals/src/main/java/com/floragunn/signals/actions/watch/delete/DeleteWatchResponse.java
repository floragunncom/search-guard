package com.floragunn.signals.actions.watch.delete;

import java.io.IOException;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

public class DeleteWatchResponse extends ActionResponse implements ToXContentObject {

    private String id;
    private long version;
    private Result result;
    private RestStatus restStatus;
    private String message;
    
    public DeleteWatchResponse() {
    }

    public DeleteWatchResponse(String id, long version,  Result result, RestStatus restStatus, String message) {
        this.id = id;
        this.version = version;
        this.result = result;
        this.restStatus = restStatus;
        this.message = message;
    }

    public DeleteWatchResponse(StreamInput in) throws IOException {
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
