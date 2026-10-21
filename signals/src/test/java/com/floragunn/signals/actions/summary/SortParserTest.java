/*
 * Copyright 2023-2026 floragunn GmbH
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

import com.floragunn.searchsupport.util.EsLogging;
import com.floragunn.signals.actions.summary.SortParser.SortByField;
import org.elasticsearch.ElasticsearchStatusException;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class SortParserTest {

    @ClassRule
    public static EsLogging esLogging = new EsLogging();

    @Test
    public void shouldHandleEmptySortingParam() {
        List<SortByField> sorting = SortParser.parseSortingExpression("");

        assertThat(sorting, hasSize(0));
    }

    @Test
    public void shouldHandleNullSortingParam() {
        List<SortByField> sorting = SortParser.parseSortingExpression(null);

        assertThat(sorting, hasSize(0));
    }

    @Test
    public void shouldHandleBlankSortingParam() {
        List<SortByField> sorting = SortParser.parseSortingExpression("   \t    ");

        assertThat(sorting, hasSize(0));
    }

    @Test
    public void shouldSortBySeverityByDefaultOrder() {
        List<SortByField> sorting = SortParser.parseSortingExpression("severity");

        assertThat(sorting, hasSize(1));
        SortByField field = sorting.get(0);
        assertThat(field.isAscending(), equalTo(true));
        assertThat(field.getDocumentFieldName(), equalTo("last_status.severity.keyword"));
    }

    @Test
    public void shouldSortBySeverityByAscOrder() {
        List<SortByField> sorting = SortParser.parseSortingExpression("+severity");

        assertThat(sorting, hasSize(1));
        SortByField field = sorting.get(0);
        assertThat(field.isAscending(), equalTo(true));
        assertThat(field.getDocumentFieldName(), equalTo("last_status.severity.keyword"));
    }

    @Test
    public void shouldSortBySeverityByDescOrder() {
        List<SortByField> sorting = SortParser.parseSortingExpression("-severity");

        assertThat(sorting, hasSize(1));
        SortByField field = sorting.get(0);
        assertThat(field.isAscending(), equalTo(false));
        assertThat(field.getDocumentFieldName(), equalTo("last_status.severity.keyword"));
    }

    @Test(expected = ElasticsearchStatusException.class)
    public void shouldReportExceptionOnSortingByUnknownField() {
        SortParser.parseSortingExpression("unknown_field");
    }

    @Test(expected = ElasticsearchStatusException.class)
    public void shouldReportExceptionOnSortingByOnlyAscSortMarker() {
        SortParser.parseSortingExpression("+");
    }

    @Test(expected = ElasticsearchStatusException.class)
    public void shouldReportExceptionOnSortingByOnlyDescSortMarker() {
        SortParser.parseSortingExpression("-");
    }

    @Test
    public void shouldSortByMultipleFields() {
        List<SortByField> sorting = SortParser.parseSortingExpression("-severity,+status_code,severity_details.level_numeric");

        assertThat(sorting, hasSize(3));
        assertThat(sorting.get(0).isAscending(), equalTo(false));
        assertThat(sorting.get(0).getDocumentFieldName(), equalTo("last_status.severity.keyword"));

        assertThat(sorting.get(1).isAscending(), equalTo(true));
        assertThat(sorting.get(1).getDocumentFieldName(), equalTo("last_status.code.keyword"));

        assertThat(sorting.get(2).isAscending(), equalTo(true));
        assertThat(sorting.get(2).getDocumentFieldName(), equalTo("last_execution.severity.level_numeric"));
    }
}