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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@RunWith(Parameterized.class)
public class LiteralSummaryToWatchFieldMapperTest {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {"severity", "last_status.severity.keyword"},
            {"status_code", "last_status.code.keyword"},
            {"severity_details.level_numeric", "last_execution.severity.level_numeric"},
            {"severity_details.current_value", "last_execution.severity.value"},
            {"severity_details.threshold", "last_execution.severity.threshold"},
            {"actions.status_code", "last_execution.severity.threshold"},
        });
    }

    private final String inputFieldName;
    private final String outputFieldName;

    public LiteralSummaryToWatchFieldMapperTest(String inputFieldName, String outputFieldName) {
        this.inputFieldName = inputFieldName;
        this.outputFieldName = outputFieldName;
    }

    @Test
    public void shouldMapLiteralFieldNames() {
        Optional<SummaryToWatchFieldMapper> foundField = SummaryToWatchFieldMapper.findFieldByName(inputFieldName);

        assertThat(foundField.isPresent(), equalTo(true));
        SummaryToWatchFieldMapper summaryField = foundField.get();
        String documentFieldName = summaryField.getDocumentFieldName(inputFieldName);
        assertThat(documentFieldName, equalTo(outputFieldName));
    }
}