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
package com.floragunn.signals.watch.common.auth;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.codova.validation.errors.MissingAttribute;
import org.elasticsearch.xcontent.ToXContentObject;

public abstract class Auth implements ToXContentObject {

    public static final String INCLUDE_CREDENTIALS = "INCLUDE_CREDENTIALS";

    public static Auth create(DocNode jsonNode) throws ConfigValidationException {

        if (!jsonNode.hasNonNull("type")) {
            throw new ConfigValidationException(new MissingAttribute("type", jsonNode));
        }

        String type = jsonNode.getAsString("type");

        switch (type) {
        case "basic":
            return BasicAuth.create(jsonNode);

        default:
            throw new ConfigValidationException(new InvalidAttributeValue("type", type, "basic", jsonNode));
        }
    }
}
