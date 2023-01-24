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
package com.floragunn.signals.watch.result;

import java.io.IOException;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

public class ErrorInfo implements ToXContentObject {

    private String check;
    private String message;
    private ToXContent detail;

    public ErrorInfo(String check, String message, ToXContent detail) {
        this.check = check;
        this.message = message;
        this.detail = detail;
    }

    public String getCheck() {
        return check;
    }

    public void setCheck(String check) {
        this.check = check;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public ToXContent getDetail() {
        return detail;
    }

    public void setDetail(ToXContent detail) {
        this.detail = detail;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (check != null) {
            builder.field("check", check);
        }

        builder.field("message", message);

        if (detail != null) {
            builder.field("detail");

            if (detail.isFragment()) {
                builder.startObject();
                detail.toXContent(builder, params);
                builder.endObject();
            } else {
                detail.toXContent(builder, params);
            }
        }

        builder.endObject();
        return builder;
    }

}
