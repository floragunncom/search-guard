/*
 * Copyright 2021 floragunn GmbH
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

package com.floragunn.codova.documents;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentType;

import com.fasterxml.jackson.core.JsonProcessingException;

public class UnparsedDoc implements Writeable {
    private final String source;
    private final XContentType contentType;

    public UnparsedDoc(String source, XContentType contentType) {
        this.source = source;
        this.contentType = contentType;
    }

    public UnparsedDoc(StreamInput in) throws IOException {
        this.source = in.readString();
        this.contentType = in.readEnum(XContentType.class);
    }

    public Map<String, Object> parseAsMap() throws JsonProcessingException {
        return DocReader.readObject(source, contentType);
    }

    public Object parse() throws JsonProcessingException {
        return DocReader.read(source, contentType);
    }

    public String getSource() {
        return source;
    }

    public XContentType getContentType() {
        return contentType;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(source);
        out.writeEnum(contentType);
    }

}
