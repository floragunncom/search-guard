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
        assertThat(errors.get("start_date").iterator().next().getExpected(), equalTo("Date no earlier than 30 days before today"));
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
