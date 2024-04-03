/*
 * Copyright 2021-2024 floragunn GmbH
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

package com.floragunn.searchguard.authz;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.support.ConfigConstants;

public class DocumentWhitelist {
    private static final Logger log = LogManager.getLogger(DocumentWhitelist.class);

    public static DocumentWhitelist get(ThreadContext threadContext) {
        String docWhitelistHeader = threadContext.getHeader(ConfigConstants.SG_DOC_WHITELST_HEADER);

        if (docWhitelistHeader == null) {
            return EMPTY;
        } else {
            try {

                return DocumentWhitelist.parse(docWhitelistHeader);
            } catch (Exception e) {
                log.error("Error while handling document whitelist: " + docWhitelistHeader, e);
                return EMPTY;
            }
        }
    }

    private static final DocumentWhitelist EMPTY = new DocumentWhitelist(ImmutableSet.empty());

    private final ImmutableSet<Entry> entries;

    public DocumentWhitelist(ImmutableSet<DocumentWhitelist.Entry> entries) {
        this.entries = entries;
    }

    public boolean isEmpty() {
        return this.entries.isEmpty();
    }

    public void applyTo(ThreadContext threadContext) {
        if (!isEmpty()) {
            threadContext.putHeader(ConfigConstants.SG_DOC_WHITELST_HEADER, toString());
        }
    }

    public boolean isWhitelisted(String index, String id) {
        for (Entry entry : entries) {
            if (entry.index.equals(index) && entry.id.equals(id)) {
                return true;
            }
        }

        return false;
    }
    
    public boolean isWhitelistForIndexPresent(String index) {
        for (Entry entry : entries) {
            if (entry.index.equals(index)) {
                return true;
            }
        }

        return false;
    }

    public String toString() {
        if (this.entries.isEmpty()) {
            return "";
        }

        StringBuilder stringBuilder = new StringBuilder();

        for (Entry entry : entries) {
            if (stringBuilder.length() != 0) {
                stringBuilder.append('|');
            }
            stringBuilder.append(entry.index).append("/").append(escapeId(entry.id));
        }

        return stringBuilder.toString();
    }

    public static DocumentWhitelist parse(String string) {
        int length = string.length();

        if (length == 0) {
            return EMPTY;
        }

        ImmutableSet.Builder<Entry> entries = new ImmutableSet.Builder<>();

        int entryStart = 0;
        String index = null;

        for (int i = 0;; i++) {
            char c;

            if (i < length) {
                c = string.charAt(i);
            } else {
                c = '|';
            }

            while (c == '\\') {
                i += 2;
                c = string.charAt(i);
            }

            if (c == '/') {
                index = string.substring(entryStart, i);
                entryStart = i + 1;
            } else if (c == '|') {
                if (index == null) {
                    throw new IllegalArgumentException("Malformed DocumentWhitelist string: " + string);
                }

                String id = unescapeId(string.substring(entryStart, i));

                entries.add(new Entry(index, id));
                index = null;
                entryStart = i + 1;
            }

            if (i >= length) {
                break;
            }
        }

        return new DocumentWhitelist(entries.build());
    }

    private static String escapeId(String id) {
        int length = id.length();
        boolean needsEscaping = false;

        for (int i = 0; i < length; i++) {
            char c = id.charAt(i);
            if (c == '/' || c == '|' || c == '\\') {
                needsEscaping = true;
                break;
            }
        }

        if (!needsEscaping) {
            return id;
        }

        StringBuilder result = new StringBuilder(id.length() + 10);

        for (int i = 0; i < length; i++) {
            char c = id.charAt(i);
            if (c == '/' || c == '|' || c == '\\') {
                result.append('\\');
            }
            result.append(c);
        }

        return result.toString();
    }

    private static String unescapeId(String id) {
        int length = id.length();
        boolean needsEscaping = false;

        for (int i = 0; i < length; i++) {
            char c = id.charAt(i);
            if (c == '\\') {
                needsEscaping = true;
                break;
            }
        }

        if (!needsEscaping) {
            return id;
        }

        StringBuilder result = new StringBuilder(id.length());

        for (int i = 0; i < length; i++) {
            char c = id.charAt(i);
            if (c == '\\') {
                i++;
                c = id.charAt(i);
            }

            result.append(c);
        }

        return result.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((entries == null) ? 0 : entries.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        DocumentWhitelist other = (DocumentWhitelist) obj;
        if (entries == null) {
            if (other.entries != null) {
                return false;
            }
        } else if (!entries.equals(other.entries)) {
            return false;
        }
        return true;
    }

    public static class Entry {
        private final String index;
        private final String id;

        Entry(String index, String id) {
            if (index.indexOf('/') != -1 || index.indexOf('|') != -1) {
                throw new IllegalArgumentException("Invalid index name: " + index);
            }

            this.index = index;
            this.id = id;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((id == null) ? 0 : id.hashCode());
            result = prime * result + ((index == null) ? 0 : index.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Entry other = (Entry) obj;
            if (id == null) {
                if (other.id != null) {
                    return false;
                }
            } else if (!id.equals(other.id)) {
                return false;
            }
            if (index == null) {
                if (other.index != null) {
                    return false;
                }
            } else if (!index.equals(other.index)) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return "DocumentWhitelist.Entry [index=" + index + ", id=" + id + "]";
        }
    }

    public static class Builder {
        private ImmutableSet.Builder<Entry> entries = new ImmutableSet.Builder<>();

        public void add(String index, String id) {
            this.add(new Entry(index, id));
        }

        public void add(Entry entry) {
            this.entries.add(entry);
        }

        public DocumentWhitelist build() {
            return new DocumentWhitelist(this.entries.build());
        }
    }

}
