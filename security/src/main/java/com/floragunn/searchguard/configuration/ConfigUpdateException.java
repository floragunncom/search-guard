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

import org.opensearch.common.Strings;
import org.opensearch.common.xcontent.ToXContentObject;

import com.floragunn.codova.documents.DocWriter;
import com.google.common.collect.ImmutableMap;

public class ConfigUpdateException extends Exception {

    private static final long serialVersionUID = -8627848154509219672L;
    private Object details;
    
    public ConfigUpdateException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConfigUpdateException(String message) {
        super(message);
    }
    
    public ConfigUpdateException(String message, Object details) {
        super(message);
        this.details = details;
    }
    
    public ConfigUpdateException(Throwable cause) {
        super(cause);
    }

    public Object getDetails() {
        return details;
    }
    
    public String getDetailsAsJson() {
        if (details instanceof ToXContentObject) {
            return Strings.toString((ToXContentObject) details);
        } else if (details != null) {
            return DocWriter.json().writeAsString(ImmutableMap.of("object", details.toString()));
        } else {
            return null;
        }
    }
}
