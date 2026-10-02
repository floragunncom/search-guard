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
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
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
    public void validate_shouldReturnErrorWhenCurrentDateDoesNotFallsWithinThirtyDaysBeforeStartDate() {
        LocalDate startDate = LocalDate.now().plusDays(40);
        String issueDate = dateToString(startDate.minusDays(50));
        String expiryDate = dateToString(startDate.plusDays(30));
        String startDateStr = dateToString(startDate);
        SearchGuardLicense searchGuardLicense = new SearchGuardLicense(UUID.randomUUID().toString(), SearchGuardLicense.Type.FULL,
                SearchGuardLicense.Feature.values(), issueDate, expiryDate, "abc", "SG", startDateStr,
                7, "abc", 1
        );

        ValidationErrors validationErrors = searchGuardLicense.staticValidate();

        Map<String, Collection<ValidationError>> errors = validationErrors.getErrors();
        assertThat(errors.size(), equalTo(1));
        assertThat(errors, hasEntry(equalTo("start_date"), anything()));
        assertThat(errors, hasKey(equalTo("start_date")));
        assertThat(errors.get("start_date"), hasSize(1));
        assertThat(errors.get("start_date").iterator().next().getMessage(), equalTo("License cannot be applied earlier than " + DateTimeFormatter.ISO_DATE.format(startDate.minusDays(30))));
    }

    @Test
    public void validate_shouldNotReturnErrorWhenCurrentDateFallsWithinThirtyDaysBeforeStartDate() {
        LocalDate startDate = LocalDate.now().plusDays(25);
        String issueDate = dateToString(startDate.minusDays(50));
        String expiryDate = dateToString(startDate.plusDays(30));
        String startDateStr = dateToString(startDate);
        SearchGuardLicense searchGuardLicense = new SearchGuardLicense(UUID.randomUUID().toString(), SearchGuardLicense.Type.FULL,
                SearchGuardLicense.Feature.values(), issueDate, expiryDate, "abc", "SG", startDateStr,
                7, "abc", 1
        );

        ValidationErrors validationErrors = searchGuardLicense.staticValidate();

        Map<String, Collection<ValidationError>> errors = validationErrors.getErrors();
        assertThat(errors.size(), equalTo(0));
    }

    private String dateToString(LocalDate date) {
        return DateTimeFormatter.ISO_DATE.format(date);
    }

    @Test
    public void mapConstructor_licenseKeyFormat() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("uid", "B83FCDC8-88F7-45B0-9616-73108FA05CCD");
        map.put("type", "FULL");
        map.put("features", Arrays.asList("COMPLIANCE"));
        map.put("issued_date", "2018-04-26");
        map.put("expiry_date", "2038-01-31");
        map.put("issued_to", "Mustermann GmbH");
        map.put("issuer", "floragunn GmbH");
        map.put("start_date", "2017-01-31");
        map.put("major_version", 6);
        map.put("cluster_name", "*");
        map.put("allowed_node_count_per_cluster", 32768);
        map.put("license_version", 1);

        SearchGuardLicense license = new SearchGuardLicense(map);

        assertThat(license.getUid(), equalTo("B83FCDC8-88F7-45B0-9616-73108FA05CCD"));
        assertThat(license.getType(), equalTo(SearchGuardLicense.Type.FULL));
        assertThat(license.getFeatures(), equalTo(new SearchGuardLicense.Feature[] { SearchGuardLicense.Feature.COMPLIANCE }));
        assertThat(license.getIssueDate(), equalTo("2018-04-26"));
        assertThat(license.getExpiryDate(), equalTo("2038-01-31"));
        assertThat(license.getIssuedTo(), equalTo("Mustermann GmbH"));
        assertThat(license.getIssuer(), equalTo("floragunn GmbH"));
        assertThat(license.getStartDate(), equalTo("2017-01-31"));
        assertThat(license.getMajorVersion(), equalTo(6));
        assertThat(license.getClusterName(), equalTo("*"));
        assertThat(license.getAllowedNodeCount(), equalTo(32768));
    }

    @Test
    public void mapConstructor_roundTripOfToBasicObject_unlimitedNodes() {
        SearchGuardLicense original = SearchGuardLicense.createTrialLicense("2026-01-01", null);

        @SuppressWarnings("unchecked")
        SearchGuardLicense parsed = new SearchGuardLicense((Map<String, Object>) original.toBasicObject());

        assertThat(parsed.getUid(), equalTo(original.getUid()));
        assertThat(parsed.getType(), equalTo(SearchGuardLicense.Type.TRIAL));
        assertThat(parsed.getFeatures(), equalTo(original.getFeatures()));
        assertThat(parsed.getIssueDate(), equalTo("2026-01-01"));
        assertThat(parsed.getExpiryDate(), equalTo(original.getExpiryDate()));
        assertThat(parsed.getIssuedTo(), equalTo(original.getIssuedTo()));
        assertThat(parsed.getIssuer(), equalTo(original.getIssuer()));
        assertThat(parsed.getStartDate(), equalTo(original.getStartDate()));
        assertThat(parsed.getMajorVersion(), equalTo(original.getMajorVersion()));
        assertThat(parsed.getClusterName(), equalTo(original.getClusterName()));
        assertThat(parsed.getAllowedNodeCount(), equalTo(Integer.MAX_VALUE));
    }

    @Test
    public void mapConstructor_roundTripOfToBasicObject_limitedNodes() {
        SearchGuardLicense original = new SearchGuardLicense(UUID.randomUUID().toString(), SearchGuardLicense.Type.FULL,
                new SearchGuardLicense.Feature[0], "2026-01-01", "2036-01-01", "Mustermann GmbH", "floragunn GmbH", "2026-01-01", 7, "*", 32);

        @SuppressWarnings("unchecked")
        SearchGuardLicense parsed = new SearchGuardLicense((Map<String, Object>) original.toBasicObject());

        assertThat(parsed.getUid(), equalTo(original.getUid()));
        assertThat(parsed.getType(), equalTo(SearchGuardLicense.Type.FULL));
        assertThat(parsed.getIssueDate(), equalTo("2026-01-01"));
        assertThat(parsed.getMajorVersion(), equalTo(7));
        assertThat(parsed.getAllowedNodeCount(), equalTo(32));
    }
}
