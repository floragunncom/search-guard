package com.floragunn.signals.actions.watch.get;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import com.floragunn.signals.watch.checks.StaticInput;

public class GetWatchResponse extends ActionResponse implements ToXContentObject {

    private String tenant;
    private String id;
    private boolean exists;
    private long version;
    private long seqNo;
    private long primaryTerm;
    private BytesReference source;

    public GetWatchResponse() {
    }

    public GetWatchResponse(String tenant, String id, boolean exists) {
        this(tenant, id, exists, -1, -1, -1, null);
    }

    public GetWatchResponse(String tenant, String id, boolean exists, long version, long seqNo, long primaryTerm, BytesReference source) {
        super();
        this.tenant = tenant;
        this.id = id;
        this.exists = exists;
        this.version = version;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.source = source;
    }

    public GetWatchResponse(String tenant, GetResponse getResponse) {
        this.tenant = tenant;
        this.id = getResponse.getId();
        this.exists = getResponse.isExists();
        this.version = getResponse.getVersion();
        this.seqNo = getResponse.getSeqNo();
        this.primaryTerm = getResponse.getPrimaryTerm();
        this.source = getResponse.getSourceAsBytesRef();
    }

    public GetWatchResponse(StreamInput in) throws IOException {
        this.tenant = in.readOptionalString();
        this.id = in.readString();
        this.exists = in.readBoolean();
        this.version = in.readLong();
        this.seqNo = in.readLong();
        this.primaryTerm = in.readLong();
        this.source = in.readOptionalBytesReference();

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(tenant);
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
        builder.field("_tenant", tenant);
        builder.field("found", exists);

        if (exists) {
            builder.field("_version", version);
            builder.field("_seq_no", seqNo);
            builder.field("_primary_term", primaryTerm);
            
            Tuple<XContentType, Map<String, Object>> parsedSource = XContentHelper.convertToMap(source,  true, XContentType.JSON);
            Map<String, Object> source = new LinkedHashMap<>(parsedSource.v2());
            
            StaticInput.unpatchForIndexMappingBugFix(source);
            
            builder.field(SourceFieldMapper.NAME, source);
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

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }
}
