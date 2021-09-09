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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.floragunn.codova.documents.DocType.UnknownContentTypeException;

public class UnparsedDoc {
    private final String source;
    private final DocType docType;

    public UnparsedDoc(String source, DocType docType) {
        this.source = source;
        this.docType = docType;
    }

    public Map<String, Object> parseAsMap() throws JsonProcessingException {
        return DocReader.type(docType).readObject(source);
    }

    public Object parse() throws JsonProcessingException {
        return DocReader.type(docType).read(source);
    }

    public String getSource() {
        return source;
    }

    public DocType getDocType() {
        return docType;
    }

    @Override
    public String toString() {
        return docType.getContentType() + ":\n" + source;
    }

    public static UnparsedDoc fromString(String string) throws IllegalArgumentException, UnknownContentTypeException {
        int sep = string.indexOf(":\n");

        if (sep == -1) {
            throw new IllegalArgumentException("Invalid string: " + string);
        }

        return new UnparsedDoc(string.substring(sep + 2), DocType.getByContentType(string.substring(0, sep)));
    }
}
