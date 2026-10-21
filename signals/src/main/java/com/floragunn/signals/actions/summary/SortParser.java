/*
 * Copyright 2023 floragunn GmbH
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

package com.floragunn.signals.actions.summary;

import com.google.common.base.Strings;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class SortParser {

    public static final String PREFIX_SORT_DESC = "-";
    public static final String PREFIX_SORT_ASC = "+";


    public static class SortByField {

        private final String inputName;
        private final SummaryToWatchFieldMapper mapping;
        private final boolean ascending;

        public SortByField(String inputName, SummaryToWatchFieldMapper mapping, boolean ascending) {
            this.inputName = Objects.requireNonNull(inputName, "Input field name is required");
            this.mapping = Objects.requireNonNull(mapping);
            this.ascending = ascending;
        }

        public boolean isAscending() {
            return ascending;
        }

        public String getDocumentFieldName() {
            return mapping.getDocumentFieldName(inputName);
        }

        public Optional<String> getSortingFormat() {
            return mapping.getSortingFormat();
        }
    }

    private SortParser() {
    }

    static List<SortByField> parseSortingExpression(String expression) {
        return Optional.ofNullable(expression)//
            .map(sorting -> expression.split(","))//
            .map(Arrays::stream)//
            .orElseGet(Stream::empty)//
            .filter(Objects::nonNull)//
            .map(String::trim)//
            .filter(fieldSorting -> ! Strings.isNullOrEmpty(fieldSorting))//
            .map(SortParser::parseSingleSortExpression)//
            .collect(Collectors.toList());
    }

    private static SortByField parseSingleSortExpression(String fieldSorting) {
        boolean ascending = fieldSorting.startsWith(PREFIX_SORT_DESC) ? false : true;
        String fieldName = removeSortDirectionMarker(fieldSorting);
        SummaryToWatchFieldMapper sortFieldMapping = SummaryToWatchFieldMapper.findFieldByName(fieldName) //
            .orElseThrow(() -> new ElasticsearchStatusException("Cannot sort by unknown field '" + fieldName + "'", RestStatus.BAD_REQUEST));
        return new SortByField(fieldName, sortFieldMapping, ascending);
    }

    private static String removeSortDirectionMarker(String fieldSorting) {
        if(PREFIX_SORT_DESC.equals(fieldSorting) || PREFIX_SORT_ASC.equals(fieldSorting)) {
            throw new ElasticsearchStatusException("Missing field name for sorting", RestStatus.BAD_REQUEST);
        }
        boolean shouldRemovePrefix = fieldSorting.startsWith(PREFIX_SORT_DESC) || fieldSorting.startsWith(PREFIX_SORT_ASC);
        return shouldRemovePrefix ? fieldSorting.substring(1) : fieldSorting;
    }
}
