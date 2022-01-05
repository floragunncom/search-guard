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

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;

public abstract class UnparsedDoc<T> implements Document<T> {

    public static UnparsedDoc<?> from(String source, DocType docType) {
        return new StringDoc(source, docType);
    }
    
    public static UnparsedDoc<?> from(String source, ContentType contentType) {
        return new StringDoc(source, contentType);
    }

    public static UnparsedDoc<?> from(byte[] source, ContentType contentType) {
        return new BytesDoc(source, contentType);
    }

    public static UnparsedDoc<?> from(byte[] source, DocType docType, Charset charset) {
        return new BytesDoc(source, docType, charset);
    }

    public static UnparsedDoc<?> from(byte[] source, DocType docType) {
        return new BytesDoc(source, docType, null);
    }

    public static UnparsedDoc<?> fromJson(String json) {
        return new StringDoc(json, DocType.JSON);
    }

    protected final DocType docType;
    protected final ContentType contentType;

    private UnparsedDoc(DocType docType, ContentType contentType) {
        this.docType = docType;
        this.contentType = contentType;
    }
    
    private UnparsedDoc(DocType docType) {
        this.docType = docType;
        this.contentType = docType.getContentType();
    }
    
    private UnparsedDoc(ContentType contentType) {
        this.docType = contentType.getDocType();
        this.contentType = contentType;
    }

    public abstract Map<String, Object> parseAsMap() throws DocParseException, UnexpectedDocumentStructureException;

    public abstract DocNode parseAsDocNode() throws DocParseException;

    public abstract Object parse() throws DocParseException;

    public abstract String getSourceAsString();

    public abstract JsonParser createParser() throws JsonParseException, IOException;

    public DocType getDocType() {
        return docType;
    }
    
    public ContentType getContentType() {
        return contentType;
    }
    
    public String getMediaType() {
        return contentType.getMediaType();
    }

    @Override
    public Object toBasicObject() {
        return this;
    }

    public static class StringDoc extends UnparsedDoc<Object> {
        private final String source;

        public StringDoc(String source, DocType docType) {
            super(docType);
            this.source = source;
        }
        
        public StringDoc(String source, ContentType docType) {
            super(docType);
            this.source = source;
        }

        public Map<String, Object> parseAsMap() throws DocParseException, UnexpectedDocumentStructureException {
            return DocReader.type(docType).readObject(source);
        }

        public DocNode parseAsDocNode() throws DocParseException {
            return DocNode.parse(docType).from(source);
        }

        public Object parse() throws DocParseException {
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
            return docType.getMediaType() + ":\n" + source;
        }

        @Override
        public String getSourceAsString() {
            return source;
        }

        @Override
        public JsonParser createParser() throws JsonParseException, IOException {
            return docType.getJsonFactory().createParser(source);
        }

        @Override
        public String toString(DocType docType) {
            if (docType.equals(this.docType)) {
                return source;
            } else {
                return super.toString(docType);                
            }
        }
    }

    public static class BytesDoc extends UnparsedDoc<Object> {
        private final byte[] source;
        private String sourceAsString;
        private final Charset charset;

        BytesDoc(byte[] source, ContentType contentType) {
            super(contentType);
            this.source = source;
            this.charset = contentType.getCharset();
        }
        
        BytesDoc(byte[] source, DocType docType, Charset charset) {
            super(docType);
            this.source = source;
            this.charset = charset;
        }

        public Map<String, Object> parseAsMap() throws DocParseException, UnexpectedDocumentStructureException {
            return DocReader.type(docType).readObject(source);
        }

        public DocNode parseAsDocNode() throws DocParseException {
            return DocNode.parse(docType).from(source);
        }

        public Object parse() throws DocParseException {
            return DocReader.type(docType).read(source);
        }

        public byte[] getSource() {
            return source;
        }

        public DocType getDocType() {
            return docType;
        }

        @Override
        public String toString() {
            if (docType.isBinary()) {
                return docType.getMediaType() + ": " + source.length + " bytes";
            } else {
                return docType.getMediaType() + ":\n" + getSourceAsString();
            }
        }

        @Override
        public String getSourceAsString() {
            if (sourceAsString == null) {
                sourceAsString = createSourceString();
            }

            return sourceAsString;
        }

        @Override
        public JsonParser createParser() throws JsonParseException, IOException {
            return docType.getJsonFactory().createParser(source);
        }

        private String createSourceString() {
            if (docType.isBinary()) {
                throw new IllegalStateException("Cannot encode " + docType + " as string");
            }

            if (charset != null) {
                return new String(source, charset);
            } else if (checkBom(0xff, 0xfe, 0x0, 0x0)) {
                return new String(source, 4, source.length - 4, Charset.forName("UTF-32LE"));
            } else if (checkBom(0x0, 0x0, 0xff, 0xfe)) {
                return new String(source, 4, source.length - 4, Charset.forName("UTF-32BE"));
            } else if (checkBom(0xef, 0xbb, 0xbf)) {
                return new String(source, 3, source.length - 3, StandardCharsets.UTF_8);
            } else if (checkBom(0xfe, 0xff)) {
                return new String(source, 2, source.length - 2, StandardCharsets.UTF_16BE);
            } else if (checkBom(0xff, 0xfe)) {
                return new String(source, 2, source.length - 2, StandardCharsets.UTF_16LE);
            } else {
                return new String(source, docType.getDefaultCharset());
            }
        }

        private boolean checkBom(int b1, int b2) {
            return source.length >= 2 && source[0] == b1 && source[1] == b2;
        }

        private boolean checkBom(int b1, int b2, int b3) {
            return source.length >= 3 && source[0] == b1 && source[1] == b2 && source[2] == b3;
        }

        private boolean checkBom(int b1, int b2, int b3, int b4) {
            return source.length >= 4 && source[0] == b1 && source[1] == b2 && source[2] == b3 && source[3] == b4;
        }

        @Override
        public byte[] toBytes(DocType docType) {
            if (docType.equals(this.docType)) {
                return source;
            } else {
                return super.toBytes(docType);                
            }
        }

    }

}
