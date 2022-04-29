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

package com.floragunn.searchguard.configuration;

import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.RedactableDocument;

public class SgConfigEntry<T> implements Document<T>, RedactableDocument {
    
    private final T object;
    private final SgDynamicConfiguration<T> baseConfig;

    public SgConfigEntry(T object, SgDynamicConfiguration<T> baseConfig) {
        this.object = object;
        this.baseConfig = baseConfig;
    }
    
    @Override
    public Object toRedactedBasicObject() {
        if (object instanceof RedactableDocument) {
            return ((RedactableDocument) object).toRedactedBasicObject();
        } else {
            return ((Document<?>) object).toBasicObject();
        }
    }

    @Override
    public Object toBasicObject() {
        return ((Document<?>) object).toBasicObject();
    }
    
    public String getETag() {
        return baseConfig.getETag();
    }

}
