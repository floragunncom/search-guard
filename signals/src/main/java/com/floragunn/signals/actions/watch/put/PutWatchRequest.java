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
package com.floragunn.signals.actions.watch.put;

import java.io.IOException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentType;

public class PutWatchRequest extends ActionRequest {

    private String watchId;
    private BytesReference body;
    private XContentType bodyContentType;

    public PutWatchRequest() {
        super();
    }

    public PutWatchRequest(String watchId, BytesReference body, XContentType bodyContentType) {
        super();
        this.watchId = watchId;
        this.body = body;
        this.bodyContentType = bodyContentType;
    }

    public PutWatchRequest(StreamInput in) throws IOException {
        super(in);
        this.watchId = in.readString();
        this.body = in.readBytesReference();
        this.bodyContentType = in.readEnum(XContentType.class);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(watchId);
        out.writeBytesReference(body);
        out.writeEnum(bodyContentType);
    }

    @Override
    public ActionRequestValidationException validate() {
        if (watchId == null || watchId.length() == 0) {
            return new ActionRequestValidationException();
        }
        return null;
    }

    public String getWatchId() {
        return watchId;
    }

    public void setWatchId(String watchId) {
        this.watchId = watchId;
    }

    public BytesReference getBody() {
        return body;
    }

    public void setBody(BytesReference body) {
        this.body = body;
    }

    public XContentType getBodyContentType() {
        return bodyContentType;
    }

    public void setBodyContentType(XContentType bodyContentType) {
        this.bodyContentType = bodyContentType;
    }

}
