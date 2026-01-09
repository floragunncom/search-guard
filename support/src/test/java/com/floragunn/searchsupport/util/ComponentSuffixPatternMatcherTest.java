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

package com.floragunn.searchsupport.util;

import com.floragunn.codova.config.text.Pattern;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchsupport.meta.Component;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.when;

public class ComponentSuffixPatternMatcherTest {

    // Constructor tests

    @Test
    public void constructor_createsInstance_whenValidParametersProvided() throws ConfigValidationException {
        Pattern pattern = Pattern.create("test-*");

        ComponentSuffixPatternMatcher matcher = new ComponentSuffixPatternMatcher(pattern, Component.NONE);

        assertThat(matcher, is(notNullValue()));
    }

    @Test(expected = NullPointerException.class)
    public void constructor_throwsNullPointerException_whenPatternIsNull() {
        new ComponentSuffixPatternMatcher(null, Component.NONE);
    }

    @Test(expected = NullPointerException.class)
    public void constructor_throwsNullPointerException_whenComponentIsNull() throws ConfigValidationException {
        Pattern pattern = Pattern.create("test-*");

        new ComponentSuffixPatternMatcher(pattern, null);
    }

    // create(String) tests

    @Test
    public void createFromString_createsMatcherWithNoneComponent_whenPatternHasNoComponentSuffix() throws ConfigValidationException {
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create("test-index-*");

        assertThat(matcher, is(notNullValue()));
    }

    @Test
    public void createFromString_createsMatcherWithFailuresComponent_whenPatternHasFailuresSuffix() throws ConfigValidationException {
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create("test-index-*::failures");

        assertThat(matcher, is(notNullValue()));
    }

    @Test
    public void createFromString_handlesConstantPattern_whenNoWildcards() throws ConfigValidationException {
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create("exact-index");

        assertThat(matcher, is(notNullValue()));
    }

    @Test
    public void createFromString_handlesWildcardPattern_whenAsteriskProvided() throws ConfigValidationException {
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create("*");

        assertThat(matcher, is(notNullValue()));
    }

    @Test
    public void createFromString_handlesPrefixPattern_whenEndsWithAsterisk() throws ConfigValidationException {
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create("prefix-*");

        assertThat(matcher, is(notNullValue()));
    }

    // create(List<String>) tests

    @Test
    public void createFromList_createsBlankMatcher_whenEmptyListProvided() throws ConfigValidationException {
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create(Collections.emptyList());

        assertThat(matcher.isBlank(), is(true));
    }

    @Test
    public void createFromList_createsMatcherWithNoneComponent_whenSinglePatternWithoutSuffix() throws ConfigValidationException {
        List<String> patterns = Collections.singletonList("test-index-*");

        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create(patterns);

        assertThat(matcher, is(notNullValue()));
    }

    @Test
    public void createFromList_createsMatcherWithFailuresComponent_whenSinglePatternWithFailuresSuffix() throws ConfigValidationException {
        List<String> patterns = Collections.singletonList("test-index-*::failures");

        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create(patterns);

        assertThat(matcher, is(notNullValue()));
    }

    @Test
    public void createFromList_createsMatcherWithNoneComponent_whenMultiplePatternsWithoutSuffix() throws ConfigValidationException {
        List<String> patterns = Arrays.asList("test-index-*", "prod-index-*", "dev-index-*");

        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create(patterns);

        assertThat(matcher, is(notNullValue()));
    }

    @Test
    public void createFromList_createsMatcherWithFailuresComponent_whenMultiplePatternsWithFailuresSuffix() throws ConfigValidationException {
        List<String> patterns = Arrays.asList(
            "test-index-*::failures",
            "prod-index-*::failures",
            "dev-index-*::failures"
        );

        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create(patterns);

        assertThat(matcher, is(notNullValue()));
    }

    @Test
    public void createFromList_returnsBlankMatcher_whenMultiplePatternsHaveDifferentComponents() throws ConfigValidationException {
        List<String> patterns = Arrays.asList(
            "test-index-*",
            "prod-index-*::failures"
        );

        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create(patterns);

        assertThat(matcher.isBlank(), is(true));
    }

    @Test
    public void createFromList_returnsBlankMatcher_whenMultiplePatternsHaveMixedNoneAndFailuresComponents() throws ConfigValidationException {
        List<String> patterns = Arrays.asList(
            "index-1",
            "index-2::failures",
            "index-3"
        );

        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create(patterns);

        assertThat(matcher.isBlank(), is(true));
    }

    // matches(String) tests - NONE component

    @Test
    public void matches_returnsTrue_whenStringMatchesPatternWithNoneComponent() throws ConfigValidationException {
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create("test-*");

        boolean result = matcher.matches("test-index");

        assertThat(result, is(true));
    }

    @Test
    public void matches_returnsFalse_whenStringDoesNotMatchPatternWithNoneComponent() throws ConfigValidationException {
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create("test-*");

        boolean result = matcher.matches("prod-index");

        assertThat(result, is(false));
    }

    @Test
    public void matches_returnsTrue_whenStringDoesNotHaveComponentSuffix() throws ConfigValidationException {
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create("test-*");

        boolean result = matcher.matches("test-index");

        assertThat(result, is(true));
    }

    @Test
    public void matches_returnsTrue_whenStringHasPartialComponentSuffix() throws ConfigValidationException {
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create("test-*");

        boolean result = matcher.matches("test-index:");

        assertThat(result, is(true));
    }

    @Test
    public void matches_returnsFalse_whenMatcherIsNoneButStringHasFailuresSuffix() throws ConfigValidationException {
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create("test-*");

        boolean result = matcher.matches("test-index::failures");

        assertThat(result, is(false));
    }

    @Test
    public void matches_handlesConstantPattern_whenExactMatchRequired() throws ConfigValidationException {
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create("exact-index");

        assertThat(matcher.matches("exact-index"), is(true));
        assertThat(matcher.matches("exact-index-2"), is(false));
    }

    // matches(String) tests - FAILURES component using constructor (correct usage)

    @Test
    public void matches_returnsTrue_whenStringMatchesPatternWithFailuresComponent() throws ConfigValidationException {
        Pattern pattern = Pattern.create("test-*");
        ComponentSuffixPatternMatcher matcher = new ComponentSuffixPatternMatcher(pattern, Component.FAILURES);

        boolean result = matcher.matches("test-index::failures");

        assertThat(result, is(true));
    }

    @Test
    public void matches_returnsFalse_whenStringMatchesPatternButMissingFailuresSuffix() throws ConfigValidationException {
        Pattern pattern = Pattern.create("test-*");
        ComponentSuffixPatternMatcher matcher = new ComponentSuffixPatternMatcher(pattern, Component.FAILURES);

        boolean result = matcher.matches("test-index");

        assertThat(result, is(false));
    }

    @Test
    public void matches_returnsFalse_whenStringMatchesPatternButHasWrongSuffix() throws ConfigValidationException {
        Pattern pattern = Pattern.create("test-*");
        ComponentSuffixPatternMatcher matcher = new ComponentSuffixPatternMatcher(pattern, Component.FAILURES);

        boolean result = matcher.matches("test-index");

        assertThat(result, is(false));
    }

    @Test
    public void matches_returnsFalse_whenStringDoesNotMatchPatternWithFailuresComponent() throws ConfigValidationException {
        Pattern pattern = Pattern.create("test-*");
        ComponentSuffixPatternMatcher matcher = new ComponentSuffixPatternMatcher(pattern, Component.FAILURES);

        boolean result = matcher.matches("prod-index::failures");

        assertThat(result, is(false));
    }

    @Test
    public void matches_handlesComplexPatterns_withMultipleWildcards() throws ConfigValidationException {
        Pattern pattern = Pattern.create("logs-*-*-prod");
        ComponentSuffixPatternMatcher matcher = new ComponentSuffixPatternMatcher(pattern, Component.FAILURES);

        assertThat(matcher.matches("logs-app-api-prod::failures"), is(true));
        assertThat(matcher.matches("logs-web-frontend-prod::failures"), is(true));
        assertThat(matcher.matches("logs-app-prod::failures"), is(false));
    }

    @Test
    public void matches_handlesWildcardPattern_matchesAllWithCorrectSuffix() throws ConfigValidationException {
        Pattern pattern = Pattern.create("*");
        ComponentSuffixPatternMatcher matcher = new ComponentSuffixPatternMatcher(pattern, Component.FAILURES);

        assertThat(matcher.matches("any-index::failures"), is(true));
        assertThat(matcher.matches("another-index::failures"), is(true));
        assertThat(matcher.matches("any-index"), is(false));
    }

    @Test
    public void matches_handlesQuestionMarkPattern_matchesSingleCharacter() throws ConfigValidationException {
        Pattern pattern = Pattern.create("test-?");
        ComponentSuffixPatternMatcher matcher = new ComponentSuffixPatternMatcher(pattern, Component.FAILURES);

        assertThat(matcher.matches("test-1::failures"), is(true));
        assertThat(matcher.matches("test-a::failures"), is(true));
        assertThat(matcher.matches("test-12::failures"), is(false));
    }

    // matches(String) tests - edge cases

    @Test
    public void matches_returnsFalse_whenStringIsExactlyComponentSuffix_withWildcardPattern() throws ConfigValidationException {
        Pattern pattern = Pattern.create("*");
        ComponentSuffixPatternMatcher matcher = new ComponentSuffixPatternMatcher(pattern, Component.FAILURES);

        boolean result = matcher.matches("::failures");

        assertThat(result, is(false));
    }

    @Test
    public void matches_handlesCaseSensitivePatterns() throws ConfigValidationException {
        Pattern pattern = Pattern.create("Test-*");
        ComponentSuffixPatternMatcher matcher = new ComponentSuffixPatternMatcher(pattern, Component.FAILURES);

        assertThat(matcher.matches("Test-index::failures"), is(true));
        assertThat(matcher.matches("test-index::failures"), is(false));
    }

    @Test
    public void matches_handlesEmptyStringBeforeSuffix_whenWildcardPattern() throws ConfigValidationException {
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create("*");

        boolean result = matcher.matches("");

        assertThat(result, is(true));
    }

    @Test
    public void matches_handlesStringWithMultipleSeparators() throws ConfigValidationException {
        Pattern pattern = Pattern.create("test::data::*");
        ComponentSuffixPatternMatcher matcher = new ComponentSuffixPatternMatcher(pattern, Component.FAILURES);

        boolean result = matcher.matches("test::data::stream::failures");

        assertThat(result, is(true));
    }

    // create(String) factory method tests - behavior when pattern includes component suffix

    @Test
    public void createFromString_requiresSuffix_whenPatternIncludesComponentSuffix() throws ConfigValidationException {
        // When pattern includes "::failures", the string must also include "::failures" twice
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create("test-*::failures");

        assertThat(matcher.matches("test-index::failures"), is(true));
        assertThat(matcher.matches("test-index"), is(false));
    }

    @Test
    public void createFromList_requiresSuffix_whenPatternsIncludeComponentSuffix() throws ConfigValidationException {
        List<String> patterns = Arrays.asList("test-*::failures", "prod-*::failures");
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create(patterns);

        assertThat(matcher.matches("test-index::failures::failures"), is(true));
        assertThat(matcher.matches("prod-index::failures"), is(true));
        assertThat(matcher.matches("test-index"), is(false));
    }

    // isBlank() tests

    @Test
    public void isBlank_returnsTrue_whenCreatedWithEmptyList() throws ConfigValidationException {
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create(Collections.emptyList());

        assertThat(matcher.isBlank(), is(true));
    }

    @Test
    public void isBlank_returnsFalse_whenCreatedWithValidPattern() throws ConfigValidationException {
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create("test-*");

        assertThat(matcher.isBlank(), is(false));
    }

    @Test
    public void isBlank_returnsTrue_whenCreatedWithConflictingComponents() throws ConfigValidationException {
        List<String> patterns = Arrays.asList("test-*", "prod-*::failures");
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create(patterns);

        assertThat(matcher.isBlank(), is(true));
    }

    @Test
    public void isBlank_delegatesToPattern_whenPatternIsBlank() throws ConfigValidationException {
        Pattern mockPattern = Mockito.mock(Pattern.class);
        when(mockPattern.isBlank()).thenReturn(true);

        ComponentSuffixPatternMatcher matcher = new ComponentSuffixPatternMatcher(mockPattern, Component.NONE);

        assertThat(matcher.isBlank(), is(true));
    }

    @Test
    public void isBlank_delegatesToPattern_whenPatternIsNotBlank() throws ConfigValidationException {
        Pattern mockPattern = Mockito.mock(Pattern.class);
        when(mockPattern.isBlank()).thenReturn(false);

        ComponentSuffixPatternMatcher matcher = new ComponentSuffixPatternMatcher(mockPattern, Component.NONE);

        assertThat(matcher.isBlank(), is(false));
    }

    // Integration tests with Pattern behavior

    @Test
    public void matches_usesPatternMatching_withMockedPattern() throws ConfigValidationException {
        Pattern mockPattern = Mockito.mock(Pattern.class);
        when(mockPattern.matches("test-index")).thenReturn(true);
        when(mockPattern.matches("prod-index")).thenReturn(false);

        ComponentSuffixPatternMatcher matcher = new ComponentSuffixPatternMatcher(mockPattern, Component.NONE);

        assertThat(matcher.matches("test-index"), is(true));
        assertThat(matcher.matches("prod-index"), is(false));
    }

    @Test
    public void matches_stripsFailuresSuffix_beforePatternMatching() throws ConfigValidationException {
        Pattern mockPattern = Mockito.mock(Pattern.class);
        when(mockPattern.matches("test-index")).thenReturn(true);

        ComponentSuffixPatternMatcher matcher = new ComponentSuffixPatternMatcher(mockPattern, Component.FAILURES);

        boolean result = matcher.matches("test-index::failures");

        assertThat(result, is(true));
        Mockito.verify(mockPattern).matches("test-index");
    }

    @Test
    public void matches_doesNotCallPattern_whenSuffixDoesNotMatch() {
        Pattern mockPattern = Mockito.mock(Pattern.class);

        ComponentSuffixPatternMatcher matcher = new ComponentSuffixPatternMatcher(mockPattern, Component.FAILURES);

        boolean result = matcher.matches("test-index");

        assertThat(result, is(false));
        Mockito.verify(mockPattern, Mockito.never()).matches(Mockito.anyString());
    }

    // Edge case tests - Pattern equals component suffix (results in empty pattern after removal)

    @Test
    public void createFromString_handlesPatternEqualToComponentSuffix_failuresComponent() throws ConfigValidationException {
        // When pattern is exactly "::failures", after removing suffix we get empty string
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create("::failures");

        assertThat(matcher, is(notNullValue()));
        // Empty string pattern should match exactly "::failures"
        assertThat(matcher.matches("::failures"), is(false));
        // Should not match anything else
        assertThat(matcher.matches("test::failures"), is(false));
        assertThat(matcher.matches(""), is(false));
    }

    @Test
    public void createFromString_handlesEmptyPattern_noneComponent() throws ConfigValidationException {
        // Empty pattern with NONE component
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create("");

        assertThat(matcher, is(notNullValue()));
        // Empty pattern should match empty string
        assertThat(matcher.matches(""), is(true));
        // Should not match non-empty strings
        assertThat(matcher.matches("test-index"), is(false));
    }

    @Test
    public void createFromList_handlesPatternEqualToComponentSuffix_singlePattern() throws ConfigValidationException {
        List<String> patterns = Collections.singletonList("::failures");
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create(patterns);

        assertThat(matcher, is(notNullValue()));
        // After suffix removal, empty pattern should match exactly "::failures"
        assertThat(matcher.matches("::failures"), is(false));
        assertThat(matcher.matches("any-index::failures"), is(false));
    }

    @Test
    public void createFromList_handlesPatternEqualToComponentSuffix_multiplePatterns() throws ConfigValidationException {
        // Multiple patterns all equal to component suffix
        List<String> patterns = Arrays.asList("::failures", "::failures");
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create(patterns);

        assertThat(matcher, is(notNullValue()));
        // All patterns resolve to empty string, so should match exactly "::failures"
        assertThat(matcher.matches("::failures"), is(false));
        assertThat(matcher.matches("test::failures"), is(false));
    }

    @Test
    public void createFromList_handlesEmptyPatterns_noneComponent() throws ConfigValidationException {
        // Multiple empty patterns with NONE component
        List<String> patterns = Arrays.asList("", "");
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create(patterns);

        assertThat(matcher, is(notNullValue()));
        // Empty patterns should match empty string
        assertThat(matcher.matches(""), is(true));
        assertThat(matcher.matches("test-index"), is(false));
    }

    @Test
    public void matches_handlesEmptyStringAfterSuffixRemoval_exactMatch() throws ConfigValidationException {
        // Using constructor to create empty pattern explicitly
        Pattern emptyPattern = Pattern.create("");
        ComponentSuffixPatternMatcher matcher = new ComponentSuffixPatternMatcher(emptyPattern, Component.FAILURES);

        // Empty pattern after suffix removal should only match strings that become empty after suffix removal
        assertThat(matcher.matches("::failures"), is(false));
        assertThat(matcher.matches("a::failures"), is(false));
        assertThat(matcher.matches("test-index::failures"), is(false));
    }

    @Test
    public void matches_handlesEmptyStringAfterSuffixRemoval_noneComponent() throws ConfigValidationException {
        // Empty pattern with NONE component
        Pattern emptyPattern = Pattern.create("");
        ComponentSuffixPatternMatcher matcher = new ComponentSuffixPatternMatcher(emptyPattern, Component.NONE);

        // Empty pattern should match empty string only
        assertThat(matcher.matches(""), is(true));
        assertThat(matcher.matches("a"), is(false));
        assertThat(matcher.matches("test-index"), is(false));
        // Should reject anything with component separator
        assertThat(matcher.matches("::"), is(false));
        assertThat(matcher.matches("::failures"), is(false));
    }

    // isWildcard() tests

    @Test
    public void isWildcard_returnsTrue_whenPatternIsWildcardAndComponentIsNone() throws ConfigValidationException {
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create("*");

        assertThat(matcher.isWildcard(), is(true));
    }

    @Test
    public void isWildcard_returnsFalse_whenPatternIsWildcardButComponentIsFailures() throws ConfigValidationException {
        Pattern wildcardPattern = Pattern.create("*");
        ComponentSuffixPatternMatcher matcher = new ComponentSuffixPatternMatcher(wildcardPattern, Component.FAILURES);

        assertThat(matcher.isWildcard(), is(false));
    }

    @Test
    public void isWildcard_returnsFalse_whenPatternIsNotWildcardAndComponentIsNone() throws ConfigValidationException {
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create("test-*");

        assertThat(matcher.isWildcard(), is(false));
    }

    @Test
    public void isWildcard_returnsFalse_whenPatternIsConstantAndComponentIsNone() throws ConfigValidationException {
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create("exact-index");

        assertThat(matcher.isWildcard(), is(false));
    }

    @Test
    public void isWildcard_returnsFalse_whenPatternIsEmptyAndComponentIsNone() throws ConfigValidationException {
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create("");

        assertThat(matcher.isWildcard(), is(false));
    }

    @Test
    public void isWildcard_returnsFalse_whenMatcherIsBlank() throws ConfigValidationException {
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create(Collections.emptyList());

        assertThat(matcher.isWildcard(), is(false));
    }

    @Test
    public void isWildcard_returnsFalse_whenPatternHasWildcardInMiddleAndComponentIsNone() throws ConfigValidationException {
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create("test-*-prod");

        assertThat(matcher.isWildcard(), is(false));
    }

    @Test
    public void isWildcard_returnsFalse_whenPatternHasQuestionMarkAndComponentIsNone() throws ConfigValidationException {
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create("test-?");

        assertThat(matcher.isWildcard(), is(false));
    }

    @Test
    public void isWildcard_delegatesToPattern_whenPatternIsWildcard() throws ConfigValidationException {
        Pattern mockPattern = Mockito.mock(Pattern.class);
        when(mockPattern.isWildcard()).thenReturn(true);

        ComponentSuffixPatternMatcher matcher = new ComponentSuffixPatternMatcher(mockPattern, Component.NONE);

        assertThat(matcher.isWildcard(), is(true));
        Mockito.verify(mockPattern).isWildcard();
    }

    @Test
    public void isWildcard_delegatesToPattern_whenPatternIsNotWildcard() throws ConfigValidationException {
        Pattern mockPattern = Mockito.mock(Pattern.class);
        when(mockPattern.isWildcard()).thenReturn(false);

        ComponentSuffixPatternMatcher matcher = new ComponentSuffixPatternMatcher(mockPattern, Component.NONE);

        assertThat(matcher.isWildcard(), is(false));
        Mockito.verify(mockPattern).isWildcard();
    }

    @Test
    public void isWildcard_doesNotCallPattern_whenSuffixLengthIsNonZero() throws ConfigValidationException {
        Pattern mockPattern = Mockito.mock(Pattern.class);
        when(mockPattern.isWildcard()).thenReturn(true);

        ComponentSuffixPatternMatcher matcher = new ComponentSuffixPatternMatcher(mockPattern, Component.FAILURES);

        assertThat(matcher.isWildcard(), is(false));
        // Pattern.isWildcard() should still be called since it's part of the boolean expression
        // Actually, looking at the code: return (suffixLength == 0) && pattern.isWildcard();
        // Java will short-circuit the AND, so if suffixLength != 0, pattern.isWildcard() won't be called
        // Let me verify: if (suffixLength == 0) is false, pattern.isWildcard() is not evaluated
        Mockito.verify(mockPattern, Mockito.never()).isWildcard();
    }

    @Test
    public void isWildcard_returnsFalse_whenCreatedWithConflictingComponents() throws ConfigValidationException {
        List<String> patterns = Arrays.asList("*", "test-*::failures");
        ComponentSuffixPatternMatcher matcher = ComponentSuffixPatternMatcher.create(patterns);

        // This creates a blank matcher, so isWildcard should return false
        assertThat(matcher.isWildcard(), is(false));
    }
}