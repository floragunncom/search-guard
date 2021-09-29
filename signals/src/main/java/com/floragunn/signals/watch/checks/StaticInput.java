package com.floragunn.signals.watch.checks;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.elasticsearch.common.xcontent.XContentBuilder;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.signals.execution.WatchExecutionContext;

public class StaticInput extends AbstractInput {
    private Map<String, Object> value;

    static Check create(DocNode jsonObject) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonObject, validationErrors);

        vJsonNode.used("type");

        String name = vJsonNode.get("name").asString();
        String target = vJsonNode.get("target").asString();

        Map<String, Object> value = Collections.emptyMap();

        if (vJsonNode.hasNonNull("value")) {
            value = jsonObject.getAsNode("value").toMap();
        }

        vJsonNode.checkForUnusedAttributes();
        validationErrors.throwExceptionForPresentErrors();

        StaticInput result = new StaticInput(name, target, value);

        return result;

    }

    public StaticInput(String name, String target, Map<String, Object> value) {
        super(name, target);
        this.value = Collections.unmodifiableMap(value);
    }

    public Map<String, Object> getValue() {
        return value;
    }

    @Override
    public boolean execute(WatchExecutionContext ctx) {
        if (this.value != null) {
            ctx.getContextData().getData().put(this.target, this.value);
        }

        return true;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("type", "static");
        builder.field("name", name);
        builder.field("target", target);
        builder.field("value", value);
        builder.endObject();
        return builder;
    }
}
