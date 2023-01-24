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
package com.floragunn.signals.confconv.es;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.signals.confconv.ConversionResult;
import com.floragunn.signals.watch.checks.Check;
import com.floragunn.signals.watch.checks.StaticInput;
import java.util.ArrayList;
import java.util.List;

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
