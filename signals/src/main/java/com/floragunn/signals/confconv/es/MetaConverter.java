package com.floragunn.signals.confconv.es;

import java.util.ArrayList;
import java.util.List;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.signals.confconv.ConversionResult;
import com.floragunn.signals.watch.checks.Check;
import com.floragunn.signals.watch.checks.StaticInput;

public class MetaConverter {

    private final DocNode metaJsonNode;

    public MetaConverter(DocNode metaJsonNode) {
        this.metaJsonNode = metaJsonNode;
    }

    ConversionResult<List<Check>> convertToSignals() {
        ValidationErrors validationErrors = new ValidationErrors();
        List<Check> result = new ArrayList<>();

        if (!(metaJsonNode.isMap())) {
            validationErrors.add(new InvalidAttributeValue(null, metaJsonNode, "JSON object"));
            return new ConversionResult<List<Check>>(result, validationErrors);
        }

        result.add(new StaticInput("_imported_metadata", "_top", metaJsonNode.toMap()));

        return new ConversionResult<List<Check>>(result, validationErrors);
    }

}
