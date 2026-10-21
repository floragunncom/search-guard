/*
 * Copyright 2024 floragunn GmbH
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

package com.floragunn.searchsupport.xcontent;

import com.floragunn.fluent.collections.ImmutableList;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.xcontent.ToXContent;

import java.util.Iterator;

class TestChunkedToXContentObjectImpl implements ChunkedToXContentObject {

    private final String first;
    private final String second;

    public TestChunkedToXContentObjectImpl(String first, String second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        ToXContent firstBuilder = (builder, p) -> {
            builder.startObject();
            builder.field("first", first);
            return builder;
        };
        ToXContent secondBuilder = (builder, p) -> {
            builder.field("second", second);
            builder.endObject();
            return builder;
        };
        return ImmutableList.of(firstBuilder, secondBuilder).iterator();
    }
}
