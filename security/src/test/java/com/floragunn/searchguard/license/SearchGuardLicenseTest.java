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

package com.floragunn.searchguard.license;

import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.ValidationError;
import org.junit.Test;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;

public class SearchGuardLicenseTest {

    @Test
    public void validate_shouldReturnErrorWhenStartDateIsEarlierThanThirtyDaysBeforeToday() {
        LocalDate now = LocalDate.now();
        String issueDate = dateToString(now.minusDays(45));
        String expiryDate = dateToString(now.plusDays(30));
        String startDay = dateToString(now.minusDays(40));
        SearchGuardLicense searchGuardLicense = new SearchGuardLicense(UUID.randomUUID().toString(), SearchGuardLicense.Type.FULL,
                SearchGuardLicense.Feature.values(), issueDate, expiryDate, "abc", "SG", startDay,
                7, "abc", 1
        );

        ValidationErrors validationErrors = searchGuardLicense.staticValidate();

        Map<String, Collection<ValidationError>> errors = validationErrors.getErrors();
        assertThat(errors.size(), equalTo(1));
        assertThat(errors, hasEntry(equalTo("start_date"), anything()));
        assertThat(errors, hasKey(equalTo("start_date")));
        assertThat(errors.get("start_date"), hasSize(1));
        assertThat(errors.get("start_date").iterator().next().getMessage(), equalTo("License cannot be applied earlier than " + DateTimeFormatter.ISO_DATE.format(now.minusDays(30))));
    }

    @Test
    public void validate_shouldNotReturnAnyErrorForCorrectLicense() {
        LocalDate now = LocalDate.now();
        String issueDate = dateToString(now.minusDays(45));
        String expiryDate = dateToString(now.plusDays(30));
        String startDay = dateToString(now.minusDays(30));
        SearchGuardLicense searchGuardLicense = new SearchGuardLicense(UUID.randomUUID().toString(), SearchGuardLicense.Type.FULL,
                SearchGuardLicense.Feature.values(), issueDate, expiryDate, "abc", "SG", startDay,
                7, "abc", 1
        );

        ValidationErrors validationErrors = searchGuardLicense.staticValidate();

        Map<String, Collection<ValidationError>> errors = validationErrors.getErrors();
        assertThat(errors.size(), equalTo(0));
    }

    private String dateToString(LocalDate date) {
        return DateTimeFormatter.ISO_DATE.format(date);
    }
}
