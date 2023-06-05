package com.floragunn.signals.actions.watch.execute;

import java.io.IOException;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

/**
 * @deprecated please use {@link ExecuteGenericWatchAction} instead. Class is still needed to preserve backwards compatibility during
 * rolling upgrade.
 */
@Deprecated
public class ExecuteWatchResponse extends ActionResponse implements ToXContentObject {

    private String tenant;
    private String id;
    private Status status;
    private BytesReference result;

    public ExecuteWatchResponse() {
    }

    public ExecuteWatchResponse(String tenant, String id, Status status, BytesReference result) {
        super();
        this.tenant = tenant;
        this.id = id;
        this.status = status;
        this.result = result;
    }

    public ExecuteWatchResponse(StreamInput in) throws IOException {
        super(in);
        this.tenant = in.readOptionalString();
        this.id = in.readString();
        this.status = in.readEnum(Status.class);
        this.result = in.readOptionalBytesReference();

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(tenant);
        out.writeString(id);
        out.writeEnum(status);
        out.writeOptionalBytesReference(result);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentHelper.writeRawField(SourceFieldMapper.NAME, result, XContentType.JSON, builder, params);
        return builder;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public static enum Status {
        EXECUTED, ERROR_WHILE_EXECUTING, NOT_FOUND, TENANT_NOT_FOUND, INVALID_WATCH_DEFINITION, INVALID_INPUT, INVALID_GOTO,
        NOT_EXECUTABLE_WATCH, INSTANCE_NOT_FOUND, MISSING_WATCH
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public BytesReference getResult() {
        return result;
    }

    public void setResult(BytesReference result) {
        this.result = result;
    }
}
