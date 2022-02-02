/*
 * Copyright 2020-2022 floragunn GmbH
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

package com.floragunn.searchguard.user;

import java.util.Map;

import com.floragunn.searchsupport.util.ImmutableList;
import com.floragunn.searchsupport.util.ImmutableMap;

public interface AttributeSource {
    Object getAttributeValue(String attributeName);

    public static AttributeSource joined(AttributeSource... attributeSources) {
        final ImmutableList<AttributeSource> sources = ImmutableList.ofArray(attributeSources);

        return new AttributeSource() {

            @Override
            public Object getAttributeValue(String attributeName) {
                for (AttributeSource source : sources) {
                    Object result = source.getAttributeValue(attributeName);

                    if (result != null) {
                        return result;
                    }
                }

                return null;
            }
        };
    }

    public static AttributeSource from(Map<String, Object> map) {
        return new AttributeSource() {

            @Override
            public Object getAttributeValue(String attributeName) {
                return map.get(attributeName);
            }

        };
    }

    public static AttributeSource of(String attribute, Object value) {
        return from(ImmutableMap.of(attribute, value));
    }
}
