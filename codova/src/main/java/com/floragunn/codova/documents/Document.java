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

import java.util.Map;

public interface Document {
    Map<String, Object> toMap();

    default String toString(DocType docType) {
        return DocWriter.type(docType).writeAsString(this.toMap());
    }

    default String toJsonString() {
        return DocWriter.json().writeAsString(this.toMap());
    }

    default String toYamlString() {
        return DocWriter.yaml().writeAsString(this.toMap());
    }

}
