package com.floragunn.aim.support;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;

import java.util.HashMap;
import java.util.Map;

public class ParsingSupport {
    public static TimeValue timeValueParser(String value) {
        return TimeValue.parseTimeValue(value, "");
    }

    public static ByteSizeValue byteSizeValueParser(String value) {
        return ByteSizeValue.parseBytesSizeValue(value, "");
    }

    public static Map<String, String> stringMapParser(DocNode docNode) throws ConfigValidationException {
        ValidationErrors errors = new ValidationErrors();
        ValidatingDocNode node = new ValidatingDocNode(docNode, errors);
        HashMap<String, String> res = new HashMap<>();
        node.getDocumentNode().keySet().forEach(key -> res.put(key, node.get(key).required().asString()));
        node.checkForUnusedAttributes();
        errors.throwExceptionForPresentErrors();
        return res;
    }
}
