package com.floragunn.signals.actions.account.get;

import java.io.IOException;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

public class GetAccountResponse extends ActionResponse implements ToXContentObject {

    private String id;
    private boolean exists;
    private long version;
    private long seqNo;
    private long primaryTerm;
    private BytesReference source;

    public GetAccountResponse() {
    }

    public GetAccountResponse(String id, boolean exists) {
        this(id, exists, -1, -1, -1, null);
    }

    public GetAccountResponse(String id, boolean exists, long version, long seqNo, long primaryTerm, BytesReference source) {
        super();
        this.id = id;
        this.exists = exists;
        this.version = version;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.source = source;
    }

    public GetAccountResponse(GetResponse getResponse) {
        this.id = getResponse.getId();
        this.exists = getResponse.isExists();
        this.version = getResponse.getVersion();
        this.seqNo = getResponse.getSeqNo();
        this.primaryTerm = getResponse.getPrimaryTerm();
        this.source = getResponse.getSourceAsBytesRef();
    }

    public GetAccountResponse(StreamInput in) throws IOException {
        this.id = in.readString();
        this.exists = in.readBoolean();
        this.version = in.readLong();
        this.seqNo = in.readLong();
        this.primaryTerm = in.readLong();
        this.source = in.readOptionalBytesReference();

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeBoolean(exists);
        out.writeLong(version);
        out.writeLong(seqNo);
        out.writeLong(primaryTerm);
        out.writeOptionalBytesReference(source);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("_id", id);
        builder.field("found", exists);

        if (exists) {
            builder.field("_version", version);
            builder.field("_seq_no", seqNo);
            builder.field("_primary_term", primaryTerm);

            XContentHelper.writeRawField(SourceFieldMapper.NAME, source, XContentType.JSON, builder, params);
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

    public boolean isExists() {
        return exists;
    }

    public void setExists(boolean exists) {
        this.exists = exists;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public void setSeqNo(long seqNo) {
        this.seqNo = seqNo;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public void setPrimaryTerm(long primaryTerm) {
        this.primaryTerm = primaryTerm;
    }

    public BytesReference getSource() {
        return source;
    }

    public void setSource(BytesReference source) {
        this.source = source;
    }
}
