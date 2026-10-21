/*
 * Copyright 2024 floragunn GmbH
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

package com.floragunn.searchsupport.xcontent;

import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.xcontent.ToXContent;

/**
 * The class provide static methods which can be used to convert an object to {@link ToXContent} type. The actual conversion is performed
 * by {@link this#convertOrNull(Object)} method. To check if conversion is possible, the method {@link this#canConvert(Object)} should be
 * invoked. The object can be converted into {@link ToXContent} if one of the following interfaces is implemented by the object:
 * <ul>
 *     <li>{@link ToXContent}</li>
 *     <li>{@link ChunkedToXContent}</li>
 * </ul>
 *
 *
 * This util class should be used instead of down-casting objects to {@link ToXContent} class.
 */
public final class XContentConverter {

    private XContentConverter() {
    }

    /**
     * Check if an object can be converted to {@link ToXContent} with the usage of method {@link this#convertOrNull(Object)}
     * 
     * @param object object which is checked
     * @return <code>true</code> if conversion is possible, <code>false</code> otherwise.
     */
    public static boolean canConvert(Object object) {
        return (object instanceof ToXContent) || (object instanceof ChunkedToXContent);
    }

    /**
     * Convert an object provided as method argument to {@link ToXContent}. To check if conversion is possible
     * method {@link this#canConvert(Object)} should be used.
     *
     * @param object object to convert
     * @return object of type {@link ToXContent} if conversion is possible, otherwise <code>null</code>.
     */
    public static ToXContent convertOrNull(Object object) {
        if (object instanceof ToXContent toXContent) {
            return toXContent;
        } else if (object instanceof ChunkedToXContent chunkedToXContent) {
            return ChunkedToXContent.wrapAsToXContent(chunkedToXContent);
        } else {
            return null;
        }
    }
}
