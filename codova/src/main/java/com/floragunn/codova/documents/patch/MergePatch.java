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

package com.floragunn.codova.documents.patch;

import java.util.LinkedHashMap;
import java.util.Map;

import com.floragunn.codova.documents.DocNode;
import com.google.common.collect.ImmutableMap;

public class MergePatch implements DocPatch {

    public static final String MEDIA_TYPE = "application/merge-patch+json";

    private final DocNode patchDocument;

    public MergePatch(DocNode patchDocument) {
        this.patchDocument = patchDocument;
    }

    public DocNode apply(DocNode targetDocument) {
        return apply(targetDocument, patchDocument);
    }

    private static DocNode apply(DocNode targetDocument, DocNode patchDocument) {
        if (!patchDocument.isMap()) {
            return patchDocument;
        }

        if (!targetDocument.isMap()) {
            targetDocument = DocNode.EMPTY;
        }

        Map<String, Object> modifiedDocument = new LinkedHashMap<>(targetDocument.toNormalizedMap());

        for (Map.Entry<String, Object> entry : patchDocument.toMap().entrySet()) {
            Object targetValue = modifiedDocument.get(entry.getKey());

            if (entry.getValue() == null) {
                modifiedDocument.remove(entry.getKey());
            } else if (targetValue == null || !(entry.getValue() instanceof Map)) {
                modifiedDocument.put(entry.getKey(), entry.getValue());
            } else {
                modifiedDocument.put(entry.getKey(), apply(DocNode.wrap(targetValue), DocNode.wrap(entry.getValue())));
            }
        }

        return DocNode.wrap(modifiedDocument);
    }

    @Override
    public Object toBasicObject() {
        return patchDocument.toBasicObject();
    }

    public Object toTypedBasicObject() {
        return ImmutableMap.of("type", MEDIA_TYPE, "content", patchDocument.toBasicObject());
    }

    @Override
    public String getMediaType() {
        return MEDIA_TYPE;
    }
}
