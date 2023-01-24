/*
 * Copyright 2020-2021 floragunn GmbH
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

import com.floragunn.codova.documents.DocNode;
import com.floragunn.signals.watch.severity.SeverityLevel;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

public class Status implements ToXContentObject {
    private static final Logger log = LogManager.getLogger(Status.class);

    private Status.Code code;
    private String detail;
    private SeverityLevel severityLevel;

    public Status(Code code, String detail) {
        this.code = code;
        this.detail = detail;
    }

    public Status(Code code, SeverityLevel severityLevel, String detail) {
        this.code = code;
        this.detail = detail;
        this.severityLevel = severityLevel;
    }

    public Status.Code getCode() {
        return code;
    }

    public void setCode(Status.Code code) {
        this.code = code;
    }

    public String getDetail() {
        return detail;
    }

    public void setDetail(String detail) {
        this.detail = detail;
    }

    public SeverityLevel getSeverityLevel() {
        return severityLevel;
    }

    public void setSeverityLevel(SeverityLevel severityLevel) {
        this.severityLevel = severityLevel;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.startObject();
        builder.field("code", code);

        if (severityLevel != null) {
            builder.field("severity", severityLevel.toString());
        }

        builder.field("detail", detail);
        builder.endObject();

        return builder;
    }

    @Override
    public String toString() {
        if (detail == null && severityLevel == null) {
            return String.valueOf(code);
        } else {
            StringBuilder result = new StringBuilder(String.valueOf(code));

            if (severityLevel != null) {
                if (severityLevel == SeverityLevel.NONE) {
                    result.append(" [OK]");
                } else {
                    result.append(" [").append(severityLevel.getId().toUpperCase()).append("]");
                }
            }

            if (detail != null) {
                result.append(" ").append(detail);
            }

            return result.toString();
        }
    }

    public static Status parse(DocNode jsonNode) {
        Code code = null;
        String detail = null;
        SeverityLevel severityLevel = null;

        if (jsonNode.hasNonNull("code")) {
            code = Code.valueOf(jsonNode.getAsString("code"));
        }

        if (jsonNode.hasNonNull("detail")) {
            detail = jsonNode.getAsString("detail");
        }

        if (jsonNode.hasNonNull("severity")) {
            try {
                severityLevel = SeverityLevel.getById(jsonNode.getAsString("severity"));
            } catch (Exception e) {
                log.error("Invalid severity level in " + jsonNode + "; ignoring", e);
            }
        }

        return new Status(code, severityLevel, detail);
    }

    public static enum Code {
        EXECUTION_FAILED, NO_ACTION, ACTION_EXECUTED, SIMULATED_ACTION_EXECUTED, ACTION_FAILED, ACTION_THROTTLED, ACKED
    }

}
