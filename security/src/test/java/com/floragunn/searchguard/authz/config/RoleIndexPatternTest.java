package com.floragunn.searchguard.authz.config;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.authz.actions.Actions;
import com.floragunn.searchguard.authz.config.Role.IndexPatterns;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigurationRepository.Context;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class RoleIndexPatternTest {

    @Test
    public void shouldCreateIndexPattern() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern("[\"*\"]");

        // Basic assertions
        assertThat(indexPatterns, notNullValue());

        // Assert on pattern property
        assertThat(indexPatterns.getPattern(), notNullValue());
        assertFalse("Pattern should not be blank", indexPatterns.getPattern().isBlank());

        // Assert on source - should contain the original index pattern ["*"]
        assertThat("Source should contain exactly one pattern", indexPatterns.getSource(), hasSize(1));

        // Assert on patternTemplates - should be empty for constant pattern "*"
        assertThat("Pattern templates should be empty for constant patterns", indexPatterns.getPatternTemplates(), empty());

        // Assert on dateMathExpressions - should be empty since no date math expressions
        assertThat("Date math expressions should be empty", indexPatterns.getDateMathExpressions(), empty());

        // Assert on toString - should return a meaningful string representation
        assertThat("toString should return the pattern representation", indexPatterns.toString(), notNullValue());
        assertFalse("toString should not be empty", indexPatterns.toString().isEmpty());
    }

    @Test
    public void shouldCreateIndexPatternWithMultipleConstantPatterns() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern("[\"index1\", \"index2\", \"index3\"]");

        assertThat(indexPatterns, notNullValue());
        assertThat("Pattern should not be blank for multiple patterns", indexPatterns.getPattern(), notNullValue());
        assertFalse("Pattern should not be blank", indexPatterns.getPattern().isBlank());

        // Source should contain all three patterns
        assertThat("Source should contain three patterns", indexPatterns.getSource(), hasSize(3));

        // No templates or date math expressions
        assertThat("Pattern templates should be empty", indexPatterns.getPatternTemplates(), empty());
        assertThat("Date math expressions should be empty", indexPatterns.getDateMathExpressions(), empty());

        // Verify pattern can match multiple indices
        assertTrue("Pattern should match index1", indexPatterns.getPattern().matches("index1"));
        assertTrue("Pattern should match index2", indexPatterns.getPattern().matches("index2"));
        assertTrue("Pattern should match index3", indexPatterns.getPattern().matches("index3"));
    }

    @Test
    public void shouldCreateIndexPatternWithWildcard() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern("[\"test-*\"]");

        assertThat(indexPatterns, notNullValue());
        assertThat(indexPatterns.getPattern(), notNullValue());

        // Source should contain one pattern
        assertThat(indexPatterns.getSource(), hasSize(1));

        // Verify wildcard pattern
        assertTrue("Pattern should be wildcard", indexPatterns.getPattern().matches("test-index"));
        assertTrue("Pattern should match test-123", indexPatterns.getPattern().matches("test-123"));
        assertFalse("Pattern should not match prod-index", indexPatterns.getPattern().matches("prod-index"));
    }

    @Test
    public void shouldCreateIndexPatternWithNegations() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern("[\"test-*\", \"-test-excluded\"]");

        assertThat(indexPatterns, notNullValue());
        assertThat(indexPatterns.getPattern(), notNullValue());

        // Source should contain two patterns (one positive, one negative)
        assertThat("Source should contain two patterns", indexPatterns.getSource(), hasSize(2));

        // Pattern should match test-* but exclude test-excluded
        assertTrue("Pattern should match test-allowed", indexPatterns.getPattern().matches("test-allowed"));
        assertFalse("Pattern should not match test-excluded", indexPatterns.getPattern().matches("test-excluded"));

        // No templates or date math
        assertThat(indexPatterns.getPatternTemplates(), empty());
        assertThat(indexPatterns.getDateMathExpressions(), empty());
    }

    @Test
    public void shouldCreateIndexPatternWithMultipleNegations() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern("[\"logs-*\", \"-logs-test-*\", \"-logs-dev-*\"]");

        assertThat(indexPatterns, notNullValue());
        assertThat(indexPatterns.getPattern(), notNullValue());

        // Source should contain three patterns
        assertThat("Source should contain three patterns", indexPatterns.getSource(), hasSize(3));

        // Verify pattern matching with multiple exclusions
        assertTrue("Pattern should match logs-prod", indexPatterns.getPattern().matches("logs-prod"));
        assertFalse("Pattern should not match logs-test-123", indexPatterns.getPattern().matches("logs-test-123"));
        assertFalse("Pattern should not match logs-dev-456", indexPatterns.getPattern().matches("logs-dev-456"));
    }

    @Test
    public void shouldCreateIndexPatternWithTemplate() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern("[\"user-${user.name}-*\"]");

        assertThat(indexPatterns, notNullValue());

        // Pattern should be blank for template-only patterns
        assertTrue("Pattern should be blank when only templates present", indexPatterns.getPattern().isBlank());

        // Should have one pattern template
        assertThat("Should have one pattern template", indexPatterns.getPatternTemplates(), hasSize(1));

        // Verify template structure
        Role.IndexPatterns.IndexPatternTemplate template = indexPatterns.getPatternTemplates().get(0);
        assertThat("Template should not be null", template, notNullValue());
        assertThat("Template should have a template property", template.getTemplate(), notNullValue());
        assertTrue("Exclusions should be blank", template.getExclusions().isBlank());

        // Source should contain one pattern
        assertThat(indexPatterns.getSource(), hasSize(1));

        // No date math expressions
        assertThat(indexPatterns.getDateMathExpressions(), empty());
    }

    @Test
    public void shouldCreateIndexPatternWithTemplateAndNegation() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern("[\"user-${user.name}-*\", \"-user-${user.name}-excluded\"]");

        assertThat(indexPatterns, notNullValue());

        // When both patterns contain templates, they're both added as separate templates
        // The negation template is not treated as an exclusion of the first template
        assertThat("Should have two pattern templates", indexPatterns.getPatternTemplates(), hasSize(2));

        // Source should contain two patterns
        assertThat(indexPatterns.getSource(), hasSize(2));
    }

    @Test
    public void shouldCreateIndexPatternWithDateMathExpression() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern("[\"<logs-{now/d}>\"]");

        assertThat(indexPatterns, notNullValue());

        // Pattern should be blank for date math only
        assertTrue("Pattern should be blank when only date math present", indexPatterns.getPattern().isBlank());

        // Should have one date math expression
        assertThat("Should have one date math expression", indexPatterns.getDateMathExpressions(), hasSize(1));

        // Verify date math structure
        Role.IndexPatterns.DateMathExpression dateMath = indexPatterns.getDateMathExpressions().get(0);
        assertThat("Date math should not be null", dateMath, notNullValue());
        assertThat("Date math expression should match source", dateMath.getDateMathExpression(), equalTo("<logs-{now/d}>"));
        assertTrue("Exclusions should be blank", dateMath.getExclusions().isBlank());

        // Source should contain one pattern
        assertThat(indexPatterns.getSource(), hasSize(1));

        // No pattern templates
        assertThat(indexPatterns.getPatternTemplates(), empty());
    }

    @Test
    public void shouldCreateIndexPatternWithDateMathAndNegation() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern("[\"<logs-{now/d}>\", \"-<logs-{now/d-1d}>\"]");

        assertThat(indexPatterns, notNullValue());

        // Should have one date math expression
        assertThat("Should have one date math expression", indexPatterns.getDateMathExpressions(), hasSize(1));

        // Verify date math has exclusions
        Role.IndexPatterns.DateMathExpression dateMath = indexPatterns.getDateMathExpressions().get(0);
        assertThat("Date math should have exclusions", dateMath.getExclusions(), notNullValue());
        assertFalse("Exclusions should not be blank", dateMath.getExclusions().isBlank());

        // Source should contain two patterns
        assertThat(indexPatterns.getSource(), hasSize(2));
    }

    @Test
    public void shouldCreateIndexPatternWithMixedPatterns() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern("[\"constant-*\", \"user-${user.name}-*\", \"<logs-{now/d}>\"]");

        assertThat(indexPatterns, notNullValue());

        // Should have constant pattern
        assertThat(indexPatterns.getPattern(), notNullValue());
        assertFalse("Pattern should not be blank", indexPatterns.getPattern().isBlank());
        assertTrue("Pattern should match constant-index", indexPatterns.getPattern().matches("constant-index"));

        // Should have one pattern template
        assertThat("Should have one pattern template", indexPatterns.getPatternTemplates(), hasSize(1));

        // Should have one date math expression
        assertThat("Should have one date math expression", indexPatterns.getDateMathExpressions(), hasSize(1));

        // Source should contain three patterns
        assertThat(indexPatterns.getSource(), hasSize(3));
    }

    @Test
    public void shouldCreateIndexPatternWithMixedPatternsAndNegations() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern(
            "[\"test-*\", \"-test-excluded\", \"user-${user.name}-*\", \"-constant-excluded\", \"<logs-{now/d}>\", \"-<logs-{now/d-7d}>\"]"
        );

        assertThat(indexPatterns, notNullValue());

        // Constant pattern with exclusion
        assertThat(indexPatterns.getPattern(), notNullValue());
        assertTrue("Pattern should match test-allowed", indexPatterns.getPattern().matches("test-allowed"));
        assertFalse("Pattern should not match test-excluded", indexPatterns.getPattern().matches("test-excluded"));

        // Template with exclusion - constant negations apply to templates
        assertThat("Should have one pattern template", indexPatterns.getPatternTemplates(), hasSize(1));
        Role.IndexPatterns.IndexPatternTemplate template = indexPatterns.getPatternTemplates().get(0);
        assertFalse("Template exclusions should not be blank", template.getExclusions().isBlank());

        // Date math with exclusion
        assertThat("Should have one date math expression", indexPatterns.getDateMathExpressions(), hasSize(1));
        Role.IndexPatterns.DateMathExpression dateMath = indexPatterns.getDateMathExpressions().get(0);
        assertFalse("Date math exclusions should not be blank", dateMath.getExclusions().isBlank());

        // Source should contain all six patterns
        assertThat(indexPatterns.getSource(), hasSize(6));
    }

    @Test
    public void shouldCreateIndexPatternWithLeadingNegations() throws ConfigValidationException {
        // Test case where negations come before any positive pattern
        IndexPatterns indexPatterns = createPattern("[\"-excluded1\", \"-excluded2\", \"allowed-*\"]");

        assertThat(indexPatterns, notNullValue());
        assertThat(indexPatterns.getPattern(), notNullValue());

        // Leading negations should be removed, only positive pattern should remain
        assertTrue("Pattern should match allowed-index", indexPatterns.getPattern().matches("allowed-index"));

        // Source should contain all three patterns
        assertThat(indexPatterns.getSource(), hasSize(3));
    }

    @Test
    public void shouldCreateIndexPatternWithSpecificIndex() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern("[\"specific-index\"]");

        assertThat(indexPatterns, notNullValue());
        assertThat(indexPatterns.getPattern(), notNullValue());

        // Should match exact index name
        assertTrue("Pattern should match specific-index", indexPatterns.getPattern().matches("specific-index"));
        assertFalse("Pattern should not match other-index", indexPatterns.getPattern().matches("other-index"));
        assertFalse("Pattern should not match specific-index-2", indexPatterns.getPattern().matches("specific-index-2"));
    }

    @Test
    public void shouldHandleToStringForConstantPattern() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern("[\"test-*\"]");

        String toString = indexPatterns.toString();
        assertThat("toString should not be null", toString, notNullValue());
        assertThat("toString should contain pattern", toString, containsString("test-*"));
    }

    @Test
    public void shouldHandleToStringForTemplatePattern() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern("[\"user-${user.name}-*\"]");

        String toString = indexPatterns.toString();
        assertThat("toString should not be null", toString, notNullValue());
        assertFalse("toString should not be empty", toString.isEmpty());
        // Should contain template representation
        assertThat("toString should contain template info", toString, not(equalTo("-/-")));
    }

    @Test
    public void shouldHandleToStringForDateMathPattern() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern("[\"<logs-{now/d}>\"]");

        String toString = indexPatterns.toString();
        assertThat("toString should not be null", toString, notNullValue());
        assertFalse("toString should not be empty", toString.isEmpty());
        assertThat("toString should contain date math", toString, containsString("<logs-{now/d}>"));
    }

    @Test
    public void shouldHandleToStringForMixedPatterns() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern("[\"constant-*\", \"user-${user.name}-*\"]");

        String toString = indexPatterns.toString();
        assertThat("toString should not be null", toString, notNullValue());
        assertFalse("toString should not be empty", toString.isEmpty());
        // Should contain both constant and template representations
        assertThat("toString should contain constant pattern", toString, containsString("constant-*"));
    }

    @Test
    public void shouldHandleHashCodeConsistency() throws ConfigValidationException {
        IndexPatterns indexPatterns1 = createPattern("[\"test-*\"]");
        IndexPatterns indexPatterns2 = createPattern("[\"test-*\"]");

        // Same pattern should produce same hash code
        assertEquals("Hash codes should be equal for same patterns",
                     indexPatterns1.hashCode(), indexPatterns2.hashCode());
    }

    @Test
    public void shouldHandleHashCodeDifferentPatterns() throws ConfigValidationException {
        IndexPatterns indexPatterns1 = createPattern("[\"test-*\"]");
        IndexPatterns indexPatterns2 = createPattern("[\"prod-*\"]");

        // Different patterns should (likely) produce different hash codes
        // Note: Hash collisions are possible but unlikely for these simple patterns
        assertThat("Hash codes should be different for different patterns",
                   indexPatterns1.hashCode(), not(equalTo(indexPatterns2.hashCode())));
    }

    @Test
    public void shouldHandleMultipleTemplates() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern("[\"user-${user.name}-*\", \"tenant-${user.tenant}-*\"]");

        assertThat(indexPatterns, notNullValue());

        // Should have two pattern templates
        assertThat("Should have two pattern templates", indexPatterns.getPatternTemplates(), hasSize(2));

        // Source should contain two patterns
        assertThat(indexPatterns.getSource(), hasSize(2));
    }

    @Test
    public void shouldHandleMultipleDateMathExpressions() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern("[\"<logs-{now/d}>\", \"<metrics-{now/h}>\"]");

        assertThat(indexPatterns, notNullValue());

        // Should have two date math expressions
        assertThat("Should have two date math expressions", indexPatterns.getDateMathExpressions(), hasSize(2));

        // Verify both expressions
        assertThat("First date math should match",
                   indexPatterns.getDateMathExpressions().get(0).getDateMathExpression(),
                   equalTo("<logs-{now/d}>"));
        assertThat("Second date math should match",
                   indexPatterns.getDateMathExpressions().get(1).getDateMathExpression(),
                   equalTo("<metrics-{now/h}>"));

        // Source should contain two patterns
        assertThat(indexPatterns.getSource(), hasSize(2));
    }

    @Test
    public void shouldHandleWildcardPattern() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern("[\"*\"]");

        assertThat(indexPatterns, notNullValue());
        assertTrue("Pattern should be wildcard", indexPatterns.getPattern().isWildcard());
    }

    @Test
    public void shouldHandleComplexWildcardPattern() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern("[\"*-logs-*\"]");

        assertThat(indexPatterns, notNullValue());
        assertThat(indexPatterns.getPattern(), notNullValue());

        // Verify complex wildcard matching
        assertTrue("Pattern should match test-logs-prod", indexPatterns.getPattern().matches("test-logs-prod"));
        assertTrue("Pattern should match app-logs-dev", indexPatterns.getPattern().matches("app-logs-dev"));
        assertFalse("Pattern should not match logs", indexPatterns.getPattern().matches("logs"));
    }

    @Test
    public void shouldCreateIndexPatternWithDateMathMonthRounding() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern("[\"<events-{now/M{yyyy.MM}}>\"]");

        assertThat(indexPatterns, notNullValue());

        // Pattern should be blank for date math only
        assertTrue("Pattern should be blank when only date math present", indexPatterns.getPattern().isBlank());

        // Should have one date math expression
        assertThat("Should have one date math expression", indexPatterns.getDateMathExpressions(), hasSize(1));

        // Verify date math structure with custom format
        Role.IndexPatterns.DateMathExpression dateMath = indexPatterns.getDateMathExpressions().get(0);
        assertThat("Date math should not be null", dateMath, notNullValue());
        assertThat("Date math expression should match source",
                   dateMath.getDateMathExpression(),
                   equalTo("<events-{now/M{yyyy.MM}}>"));
        assertTrue("Exclusions should be blank", dateMath.getExclusions().isBlank());

        // Source should contain one pattern
        assertThat(indexPatterns.getSource(), hasSize(1));

        // No pattern templates
        assertThat(indexPatterns.getPatternTemplates(), empty());
    }

    @Test
    public void shouldCreateIndexPatternWithDateMathMonthArithmetic() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern(
            "[\"<events-{now/M{yyyy.MM}}>\", \"<events-{now/M-1M{yyyy.MM}}>\", \"<events-{now/M-2M{yyyy.MM}}>\"]"
        );

        assertThat(indexPatterns, notNullValue());

        // Pattern should be blank for date math only
        assertTrue("Pattern should be blank when only date math present", indexPatterns.getPattern().isBlank());

        // Should have three date math expressions
        assertThat("Should have three date math expressions", indexPatterns.getDateMathExpressions(), hasSize(3));

        // Verify current month expression
        Role.IndexPatterns.DateMathExpression currentMonth = indexPatterns.getDateMathExpressions().get(0);
        assertThat("Current month expression should match",
                   currentMonth.getDateMathExpression(),
                   equalTo("<events-{now/M{yyyy.MM}}>"));
        assertTrue("Current month exclusions should be blank", currentMonth.getExclusions().isBlank());

        // Verify previous month expression (1 month ago)
        Role.IndexPatterns.DateMathExpression previousMonth = indexPatterns.getDateMathExpressions().get(1);
        assertThat("Previous month expression should match",
                   previousMonth.getDateMathExpression(),
                   equalTo("<events-{now/M-1M{yyyy.MM}}>"));
        assertTrue("Previous month exclusions should be blank", previousMonth.getExclusions().isBlank());

        // Verify two months ago expression
        Role.IndexPatterns.DateMathExpression twoMonthsAgo = indexPatterns.getDateMathExpressions().get(2);
        assertThat("Two months ago expression should match",
                   twoMonthsAgo.getDateMathExpression(),
                   equalTo("<events-{now/M-2M{yyyy.MM}}>"));
        assertTrue("Two months ago exclusions should be blank", twoMonthsAgo.getExclusions().isBlank());

        // Source should contain three patterns
        assertThat(indexPatterns.getSource(), hasSize(3));

        // No pattern templates
        assertThat(indexPatterns.getPatternTemplates(), empty());
    }

    @Test
    public void shouldCreateIndexPatternWithDateMathYearRounding() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern("[\"<logs-{now/y{yyyy}}>\"]");

        assertThat(indexPatterns, notNullValue());

        // Pattern should be blank for date math only
        assertTrue("Pattern should be blank when only date math present", indexPatterns.getPattern().isBlank());

        // Should have one date math expression
        assertThat("Should have one date math expression", indexPatterns.getDateMathExpressions(), hasSize(1));

        // Verify year rounding
        Role.IndexPatterns.DateMathExpression dateMath = indexPatterns.getDateMathExpressions().get(0);
        assertThat("Date math expression should match",
                   dateMath.getDateMathExpression(),
                   equalTo("<logs-{now/y{yyyy}}>"));
        assertTrue("Exclusions should be blank", dateMath.getExclusions().isBlank());
    }

    @Test
    public void shouldCreateIndexPatternWithDateMathWeekRounding() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern("[\"<metrics-{now/w{yyyy.ww}}>\"]");

        assertThat(indexPatterns, notNullValue());

        // Pattern should be blank for date math only
        assertTrue("Pattern should be blank when only date math present", indexPatterns.getPattern().isBlank());

        // Should have one date math expression
        assertThat("Should have one date math expression", indexPatterns.getDateMathExpressions(), hasSize(1));

        // Verify week rounding with custom format
        Role.IndexPatterns.DateMathExpression dateMath = indexPatterns.getDateMathExpressions().get(0);
        assertThat("Date math expression should match",
                   dateMath.getDateMathExpression(),
                   equalTo("<metrics-{now/w{yyyy.ww}}>"));
        assertTrue("Exclusions should be blank", dateMath.getExclusions().isBlank());
    }

    @Test
    public void shouldCreateIndexPatternWithDateMathDayArithmetic() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern(
            "[\"<logs-{now/d{yyyy.MM.dd}}>\", \"<logs-{now/d-1d{yyyy.MM.dd}}>\", \"<logs-{now/d-7d{yyyy.MM.dd}}>\"]"
        );

        assertThat(indexPatterns, notNullValue());

        // Pattern should be blank for date math only
        assertTrue("Pattern should be blank when only date math present", indexPatterns.getPattern().isBlank());

        // Should have three date math expressions
        assertThat("Should have three date math expressions", indexPatterns.getDateMathExpressions(), hasSize(3));

        // Verify today expression
        assertThat("Today expression should match",
                   indexPatterns.getDateMathExpressions().get(0).getDateMathExpression(),
                   equalTo("<logs-{now/d{yyyy.MM.dd}}>"));

        // Verify yesterday expression
        assertThat("Yesterday expression should match",
                   indexPatterns.getDateMathExpressions().get(1).getDateMathExpression(),
                   equalTo("<logs-{now/d-1d{yyyy.MM.dd}}>"));

        // Verify 7 days ago expression
        assertThat("7 days ago expression should match",
                   indexPatterns.getDateMathExpressions().get(2).getDateMathExpression(),
                   equalTo("<logs-{now/d-7d{yyyy.MM.dd}}>"));

        // Source should contain three patterns
        assertThat(indexPatterns.getSource(), hasSize(3));
    }

    @Test
    public void shouldCreateIndexPatternWithDateMathHourArithmetic() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern(
            "[\"<metrics-{now/h{yyyy.MM.dd.HH}}>\", \"<metrics-{now/h-1h{yyyy.MM.dd.HH}}>\"]"
        );

        assertThat(indexPatterns, notNullValue());

        // Pattern should be blank for date math only
        assertTrue("Pattern should be blank when only date math present", indexPatterns.getPattern().isBlank());

        // Should have two date math expressions
        assertThat("Should have two date math expressions", indexPatterns.getDateMathExpressions(), hasSize(2));

        // Verify current hour expression
        assertThat("Current hour expression should match",
                   indexPatterns.getDateMathExpressions().get(0).getDateMathExpression(),
                   equalTo("<metrics-{now/h{yyyy.MM.dd.HH}}>"));

        // Verify previous hour expression
        assertThat("Previous hour expression should match",
                   indexPatterns.getDateMathExpressions().get(1).getDateMathExpression(),
                   equalTo("<metrics-{now/h-1h{yyyy.MM.dd.HH}}>"));

        // Source should contain two patterns
        assertThat(indexPatterns.getSource(), hasSize(2));
    }

    @Test
    public void shouldCreateIndexPatternWithDateMathAddition() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern("[\"<future-{now/d+1d{yyyy.MM.dd}}>\"]");

        assertThat(indexPatterns, notNullValue());

        // Pattern should be blank for date math only
        assertTrue("Pattern should be blank when only date math present", indexPatterns.getPattern().isBlank());

        // Should have one date math expression
        assertThat("Should have one date math expression", indexPatterns.getDateMathExpressions(), hasSize(1));

        // Verify date math with addition
        Role.IndexPatterns.DateMathExpression dateMath = indexPatterns.getDateMathExpressions().get(0);
        assertThat("Date math expression with addition should match",
                   dateMath.getDateMathExpression(),
                   equalTo("<future-{now/d+1d{yyyy.MM.dd}}>"));
        assertTrue("Exclusions should be blank", dateMath.getExclusions().isBlank());
    }

    @Test
    public void shouldCreateIndexPatternWithMultipleDateMathCustomFormats() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern(
            "[\"<events-{now/M{yyyy.MM}}>\", \"<logs-{now/d{yyyy.MM.dd}}>\", \"<metrics-{now/h{HH}}>\"]"
        );

        assertThat(indexPatterns, notNullValue());

        // Pattern should be blank for date math only
        assertTrue("Pattern should be blank when only date math present", indexPatterns.getPattern().isBlank());

        // Should have three date math expressions
        assertThat("Should have three date math expressions", indexPatterns.getDateMathExpressions(), hasSize(3));

        // Verify first expression with month format
        assertThat("First expression should match",
                   indexPatterns.getDateMathExpressions().get(0).getDateMathExpression(),
                   equalTo("<events-{now/M{yyyy.MM}}>"));

        // Verify second expression with day format
        assertThat("Second expression should match",
                   indexPatterns.getDateMathExpressions().get(1).getDateMathExpression(),
                   equalTo("<logs-{now/d{yyyy.MM.dd}}>"));

        // Verify third expression with hour format
        assertThat("Third expression should match",
                   indexPatterns.getDateMathExpressions().get(2).getDateMathExpression(),
                   equalTo("<metrics-{now/h{HH}}>"));

        // Source should contain three patterns
        assertThat(indexPatterns.getSource(), hasSize(3));

        // No pattern templates
        assertThat(indexPatterns.getPatternTemplates(), empty());
    }

    @Test
    public void shouldCreateIndexPatternWithDateMathNegationsAndCustomFormat() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern(
            "[\"<logs-{now/M{yyyy.MM}}>\", \"-<logs-{now/M-3M{yyyy.MM}}>\", \"-<logs-{now/M-4M{yyyy.MM}}>\"]"
        );

        assertThat(indexPatterns, notNullValue());

        // Should have one date math expression (first positive, others are exclusions)
        assertThat("Should have one date math expression", indexPatterns.getDateMathExpressions(), hasSize(1));

        // Verify date math has exclusions
        Role.IndexPatterns.DateMathExpression dateMath = indexPatterns.getDateMathExpressions().get(0);
        assertThat("Date math expression should match",
                   dateMath.getDateMathExpression(),
                   equalTo("<logs-{now/M{yyyy.MM}}>"));
        assertFalse("Exclusions should not be blank", dateMath.getExclusions().isBlank());

        // Source should contain three patterns
        assertThat(indexPatterns.getSource(), hasSize(3));
    }

    @Test
    public void shouldCreateIndexPatternWithMixedDateMathFormats() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern(
            "[\"<daily-{now/d}>\", \"<monthly-{now/M{yyyy.MM}}>\", \"<yearly-{now/y{yyyy}}>\"]"
        );

        assertThat(indexPatterns, notNullValue());

        // Pattern should be blank for date math only
        assertTrue("Pattern should be blank when only date math present", indexPatterns.getPattern().isBlank());

        // Should have three date math expressions with different formats
        assertThat("Should have three date math expressions", indexPatterns.getDateMathExpressions(), hasSize(3));

        // Verify daily format (no custom format specified)
        assertThat("Daily format should match",
                   indexPatterns.getDateMathExpressions().get(0).getDateMathExpression(),
                   equalTo("<daily-{now/d}>"));

        // Verify monthly format with custom format
        assertThat("Monthly format should match",
                   indexPatterns.getDateMathExpressions().get(1).getDateMathExpression(),
                   equalTo("<monthly-{now/M{yyyy.MM}}>"));

        // Verify yearly format with custom format
        assertThat("Yearly format should match",
                   indexPatterns.getDateMathExpressions().get(2).getDateMathExpression(),
                   equalTo("<yearly-{now/y{yyyy}}>"));

        // Source should contain three patterns
        assertThat(indexPatterns.getSource(), hasSize(3));

        // No pattern templates
        assertThat(indexPatterns.getPatternTemplates(), empty());
    }

    @Test
    public void shouldCreateIndexPatternWithDateMathAndConstantMixedWithArithmetic() throws ConfigValidationException {
        IndexPatterns indexPatterns = createPattern(
            "[\"constant-index\", \"<events-{now/M{yyyy.MM}}>\", \"<events-{now/M-1M{yyyy.MM}}>\"]"
        );

        assertThat(indexPatterns, notNullValue());

        // Should have constant pattern
        assertThat(indexPatterns.getPattern(), notNullValue());
        assertFalse("Pattern should not be blank", indexPatterns.getPattern().isBlank());
        assertTrue("Pattern should match constant-index", indexPatterns.getPattern().matches("constant-index"));

        // Should have two date math expressions
        assertThat("Should have two date math expressions", indexPatterns.getDateMathExpressions(), hasSize(2));

        // Verify current month
        assertThat("Current month expression should match",
                   indexPatterns.getDateMathExpressions().get(0).getDateMathExpression(),
                   equalTo("<events-{now/M{yyyy.MM}}>"));

        // Verify previous month
        assertThat("Previous month expression should match",
                   indexPatterns.getDateMathExpressions().get(1).getDateMathExpression(),
                   equalTo("<events-{now/M-1M{yyyy.MM}}>"));

        // Source should contain three patterns
        assertThat(indexPatterns.getSource(), hasSize(3));

        // No pattern templates
        assertThat(indexPatterns.getPatternTemplates(), empty());
    }

    private static IndexPatterns createPattern(String patterns) throws ConfigValidationException {
        return createPattern(patterns, new Context(null, null, null, null, null, Actions.forTests()));
    }

    private static IndexPatterns createPattern(String patterns, Context parserContext) throws ConfigValidationException {
        String roleYmlDefinition = """
                test_role1:
                    index_permissions:
                        - index_patterns: !%%%PATTERN_PLACEHOLDER%%%!
                          allowed_actions:
                            - "*"
                """.replace("!%%%PATTERN_PLACEHOLDER%%%!", patterns);
        SgDynamicConfiguration<Role> roleSgDynamicConfiguration = SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML) //
                .from(roleYmlDefinition), CType.ROLES, parserContext) //
                .get();
        Role role = roleSgDynamicConfiguration.getCEntries().get("test_role1");
        Role.Index index = role.getIndexPermissions().get(0);
        return index.getIndexPatterns();
    }
}
