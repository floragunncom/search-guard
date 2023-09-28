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
