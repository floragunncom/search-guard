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

public interface RedactableDocument {
    Object toRedactedBasicObject();

    default String toRedactedString(DocType docType) {
        return DocWriter.type(docType).writeAsString(this.toRedactedBasicObject());
    }

    default String toRedactedJsonString() {
        return DocWriter.json().writeAsString(this.toRedactedBasicObject());
    }

    default String toRedactedYamlString() {
        return DocWriter.yaml().writeAsString(this.toRedactedBasicObject());
    }

    default DocNode toRedactedDocNode() {
        Object basicObject = toRedactedBasicObject();

        if (basicObject != null) {
            return DocNode.wrap(basicObject);
        } else {
            return null;
        }
    }
}
