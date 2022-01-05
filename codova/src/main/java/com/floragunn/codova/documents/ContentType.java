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

import java.nio.charset.Charset;

import com.floragunn.codova.documents.Format.UnknownDocTypeException;

public class ContentType {

    private final String contentTypeString;
    private final String mediaType;
    private final Format format;
    private final Charset charset;

    ContentType(String contentTypeString, String mediaType, Format format, Charset charset) {
        this.contentTypeString = contentTypeString;
        this.mediaType = mediaType;
        this.format = format;
        this.charset = charset;
    }

    public static ContentType parseHeader(String header) throws UnknownDocTypeException {
        if (header == null) {
            return null;
        }

        int paramSeparator = header.indexOf(';');

        if (paramSeparator == -1) {
            return new ContentType(header, header, Format.getByMediaType(header), null);
        } else {
            String mediaType = header.substring(0, paramSeparator).trim();

            int charSetPos = header.indexOf("charset=", paramSeparator);

            if (charSetPos == -1) {
                return new ContentType(header, mediaType, Format.getByMediaType(mediaType), null);
            } else {
                int nextSeparator = header.indexOf(';', charSetPos);

                return new ContentType(header, mediaType, Format.getByMediaType(mediaType),
                        Charset.forName(header.substring(charSetPos + 8, nextSeparator != -1 ? nextSeparator : header.length())));
            }
        }
    }

    public Format getFormat() {
        return format;
    }

    public Charset getCharset() {
        return charset;
    }

    @Override
    public String toString() {
        return contentTypeString;
    }

    public String getMediaType() {
        return mediaType;
    }
}
