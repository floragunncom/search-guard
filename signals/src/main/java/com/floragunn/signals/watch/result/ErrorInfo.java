package com.floragunn.signals.watch.result;

import java.io.IOException;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

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