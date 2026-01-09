/*
 * Copyright 2025 floragunn GmbH
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.never;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.floragunn.codova.config.templates.ExpressionEvaluationException;
import com.floragunn.codova.config.text.Pattern;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.authz.RoleBasedActionAuthorization.IndexPattern;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.cstate.metrics.Meter;

@RunWith(MockitoJUnitRunner.class)
public class IndexPatternTest {

    @Mock
    private User mockUser;

    @Mock
    private PrivilegesEvaluationContext mockContext;

    @Mock
    private Meter mockMeter;

    @Mock
    private Meter mockSubMeter;

    @Before
    public void setUp() {
        when(mockMeter.basic(any(String.class))).thenReturn(mockSubMeter);
    }

    @Test
    public void testSimplePatternMatches() throws PrivilegesEvaluationException, ConfigValidationException {
        Pattern pattern = Pattern.create("test-*");
        IndexPattern indexPattern = new IndexPattern(pattern, ImmutableList.empty(), ImmutableList.empty());

        assertThat(indexPattern.matches("test-index", mockUser, mockContext, mockMeter), is(true));
        assertThat(indexPattern.matches("test-123", mockUser, mockContext, mockMeter), is(true));
    }

    @Test
    public void testSimplePatternDoesNotMatch() throws PrivilegesEvaluationException, ConfigValidationException {
        Pattern pattern = Pattern.create("test-*");
        IndexPattern indexPattern = new IndexPattern(pattern, ImmutableList.empty(), ImmutableList.empty());

        assertThat(indexPattern.matches("prod-index", mockUser, mockContext, mockMeter), is(false));
        assertThat(indexPattern.matches("other", mockUser, mockContext, mockMeter), is(false));
    }

    @Test
    public void testExactPatternMatch() throws PrivilegesEvaluationException, ConfigValidationException {
        Pattern pattern = Pattern.create("exact-index");
        IndexPattern indexPattern = new IndexPattern(pattern, ImmutableList.empty(), ImmutableList.empty());

        assertThat(indexPattern.matches("exact-index", mockUser, mockContext, mockMeter), is(true));
        assertThat(indexPattern.matches("exact-index-other", mockUser, mockContext, mockMeter), is(false));
    }

    @Test
    public void testPatternTemplateMatches() throws PrivilegesEvaluationException, ExpressionEvaluationException, ConfigValidationException {
        Pattern constantPattern = Pattern.create("constant-*");
        Pattern renderedPattern = Pattern.create("user-${user.name}-*");
        Pattern exclusionPattern = Pattern.create("user-${user.name}-excluded");

        Role.IndexPatterns.IndexPatternTemplate patternTemplate = mock(Role.IndexPatterns.IndexPatternTemplate.class);
        when(patternTemplate.getExclusions()).thenReturn(exclusionPattern);

        when(mockContext.getRenderedPattern(any())).thenReturn(renderedPattern);

        IndexPattern indexPattern = new IndexPattern(constantPattern, ImmutableList.of(patternTemplate), ImmutableList.empty());

        assertThat(indexPattern.matches("user-${user.name}-data", mockUser, mockContext, mockMeter), is(true));
        verify(mockContext, times(1)).getRenderedPattern(any());
        verify(mockMeter, times(1)).basic("render_index_pattern_template");
    }

    @Test
    public void testPatternTemplateWithExclusion() throws PrivilegesEvaluationException, ExpressionEvaluationException, ConfigValidationException {
        Pattern constantPattern = Pattern.create("constant-*");
        Pattern renderedPattern = Pattern.create("user-*");
        Pattern exclusionPattern = Pattern.create("user-excluded");

        Role.IndexPatterns.IndexPatternTemplate patternTemplate = mock(Role.IndexPatterns.IndexPatternTemplate.class);
        when(patternTemplate.getExclusions()).thenReturn(exclusionPattern);

        when(mockContext.getRenderedPattern(any())).thenReturn(renderedPattern);

        IndexPattern indexPattern = new IndexPattern(constantPattern, ImmutableList.of(patternTemplate), ImmutableList.empty());

        assertThat(indexPattern.matches("user-allowed", mockUser, mockContext, mockMeter), is(true));
        assertThat(indexPattern.matches("user-excluded", mockUser, mockContext, mockMeter), is(false));
    }

    @Test(expected = PrivilegesEvaluationException.class)
    public void testPatternTemplateThrowsExpressionEvaluationException() throws PrivilegesEvaluationException, ExpressionEvaluationException, ConfigValidationException {
        Pattern constantPattern = Pattern.create("constant-*");

        Role.IndexPatterns.IndexPatternTemplate patternTemplate = mock(Role.IndexPatterns.IndexPatternTemplate.class);
        when(mockContext.getRenderedPattern(any())).thenThrow(new ExpressionEvaluationException("Test error"));

        IndexPattern indexPattern = new IndexPattern(constantPattern, ImmutableList.of(patternTemplate), ImmutableList.empty());

        indexPattern.matches("some-index", mockUser, mockContext, mockMeter);
    }

    @Test
    public void testDateMathExpressionMatches() throws PrivilegesEvaluationException, ExpressionEvaluationException, ConfigValidationException {
        Pattern constantPattern = Pattern.create("other-*");
        Pattern dateMathRendered = Pattern.create("logs-2025-01-*");
        Pattern exclusionPattern = Pattern.create("logs-excluded-*");

        Role.IndexPatterns.DateMathExpression dateMathExpression = mock(Role.IndexPatterns.DateMathExpression.class);
        when(dateMathExpression.getExclusions()).thenReturn(exclusionPattern);

        when(mockContext.getRenderedDateMathExpression(any())).thenReturn(dateMathRendered);

        IndexPattern indexPattern = new IndexPattern(constantPattern, ImmutableList.empty(), ImmutableList.of(dateMathExpression));

        assertThat(indexPattern.matches("logs-2025-01-08", mockUser, mockContext, mockMeter), is(true));
        verify(mockContext, times(1)).getRenderedDateMathExpression(any());
        verify(mockMeter, times(1)).basic("render_date_math_expression");
    }

    @Test
    public void testDateMathExpressionWithExclusion() throws PrivilegesEvaluationException, ExpressionEvaluationException, ConfigValidationException {
        Pattern constantPattern = Pattern.create("other-*");
        Pattern dateMathRendered = Pattern.create("logs-*");
        Pattern exclusionPattern = Pattern.create("logs-excluded-*");

        Role.IndexPatterns.DateMathExpression dateMathExpression = mock(Role.IndexPatterns.DateMathExpression.class);
        when(dateMathExpression.getExclusions()).thenReturn(exclusionPattern);

        when(mockContext.getRenderedDateMathExpression(any())).thenReturn(dateMathRendered);

        IndexPattern indexPattern = new IndexPattern(constantPattern, ImmutableList.empty(), ImmutableList.of(dateMathExpression));

        assertThat(indexPattern.matches("logs-allowed", mockUser, mockContext, mockMeter), is(true));
        assertThat(indexPattern.matches("logs-excluded-123", mockUser, mockContext, mockMeter), is(false));
    }

    @Test(expected = PrivilegesEvaluationException.class)
    public void testDateMathExpressionThrowsException() throws PrivilegesEvaluationException, ExpressionEvaluationException, ConfigValidationException {
        Pattern constantPattern = Pattern.create("logs-*");

        Role.IndexPatterns.DateMathExpression dateMathExpression = mock(Role.IndexPatterns.DateMathExpression.class);
        when(mockContext.getRenderedDateMathExpression(any())).thenThrow(new RuntimeException("Date math error"));

        IndexPattern indexPattern = new IndexPattern(constantPattern, ImmutableList.empty(), ImmutableList.of(dateMathExpression));

        indexPattern.matches("some-index", mockUser, mockContext, mockMeter);
    }

    @Test
    public void testMatchesIterableWithMatchingIndex() throws PrivilegesEvaluationException, ConfigValidationException {
        Pattern pattern = Pattern.create("test-*");
        IndexPattern indexPattern = new IndexPattern(pattern, ImmutableList.empty(), ImmutableList.empty());

        Iterable<String> indices = Arrays.asList("prod-index", "test-index", "other-index");

        assertThat(indexPattern.matches(indices, mockUser, mockContext, mockMeter), is(true));
    }

    @Test
    public void testMatchesIterableWithNoMatchingIndex() throws PrivilegesEvaluationException, ConfigValidationException {
        Pattern pattern = Pattern.create("test-*");
        IndexPattern indexPattern = new IndexPattern(pattern, ImmutableList.empty(), ImmutableList.empty());

        Iterable<String> indices = Arrays.asList("prod-index", "dev-index", "other-index");

        assertThat(indexPattern.matches(indices, mockUser, mockContext, mockMeter), is(false));
    }

    @Test
    public void testMatchesIterableWithEmptyList() throws PrivilegesEvaluationException, ConfigValidationException {
        Pattern pattern = Pattern.create("test-*");
        IndexPattern indexPattern = new IndexPattern(pattern, ImmutableList.empty(), ImmutableList.empty());

        assertThat(indexPattern.matches(Collections.emptyList(), mockUser, mockContext, mockMeter), is(false));
    }

    @Test
    public void testMatchesIterableMatchesFirstElement() throws PrivilegesEvaluationException, ConfigValidationException {
        Pattern pattern = Pattern.create("test-*");
        IndexPattern indexPattern = new IndexPattern(pattern, ImmutableList.empty(), ImmutableList.empty());

        Iterable<String> indices = Arrays.asList("test-first", "other-index", "another-index");

        assertThat(indexPattern.matches(indices, mockUser, mockContext, mockMeter), is(true));
    }

    @Test
    public void testToStringWithPattern() throws ConfigValidationException {
        Pattern pattern = Pattern.create("test-*");
        IndexPattern indexPattern = new IndexPattern(pattern, ImmutableList.empty(), ImmutableList.empty());

        assertThat(indexPattern.toString(), is(equalTo(pattern.toString())));
    }

    @Test
    public void testToStringWithPatternAndTemplates() throws ConfigValidationException {
        Pattern pattern = Pattern.create("test-*");
        Role.IndexPatterns.IndexPatternTemplate template = mock(Role.IndexPatterns.IndexPatternTemplate.class);
        ImmutableList<Role.IndexPatterns.IndexPatternTemplate> templates = ImmutableList.of(template);

        IndexPattern indexPattern = new IndexPattern(pattern, templates, ImmutableList.empty());

        String result = indexPattern.toString();
        assertThat(result, containsString(pattern.toString()));
    }

    @Test
    public void testToStringWithOnlyTemplates() {
        Role.IndexPatterns.IndexPatternTemplate template = mock(Role.IndexPatterns.IndexPatternTemplate.class);
        ImmutableList<Role.IndexPatterns.IndexPatternTemplate> templates = ImmutableList.of(template);

        IndexPattern indexPattern = new IndexPattern(null, templates, ImmutableList.empty());

        assertThat(indexPattern.toString(), is(equalTo(templates.toString())));
    }

    @Test
    public void testToStringWithNullValues() {
        IndexPattern indexPattern = new IndexPattern(null, null, null);

        assertThat(indexPattern.toString(), is(equalTo("-/-")));
    }

    @Test
    public void testBuilderAddAndBuild() throws ConfigValidationException {
        Role.IndexPatterns roleIndexPattern = mock(Role.IndexPatterns.class);
        Pattern pattern = Pattern.create("test-*");
        Role.IndexPatterns.IndexPatternTemplate template = mock(Role.IndexPatterns.IndexPatternTemplate.class);
        Role.IndexPatterns.DateMathExpression dateMath = mock(Role.IndexPatterns.DateMathExpression.class);

        when(roleIndexPattern.getPattern()).thenReturn(pattern);
        when(roleIndexPattern.getPatternTemplates()).thenReturn(ImmutableList.of(template));
        when(roleIndexPattern.getDateMathExpressions()).thenReturn(ImmutableList.of(dateMath));

        IndexPattern.Builder builder = new IndexPattern.Builder();
        builder.add(roleIndexPattern);
        IndexPattern indexPattern = builder.build();

        assertThat(indexPattern != null, is(true));
        verify(roleIndexPattern, times(1)).getPattern();
        verify(roleIndexPattern, times(1)).getPatternTemplates();
        verify(roleIndexPattern, times(1)).getDateMathExpressions();
    }

    @Test
    public void testBuilderWithMultipleAdds() throws ConfigValidationException {
        Role.IndexPatterns roleIndexPattern1 = mock(Role.IndexPatterns.class);
        Role.IndexPatterns roleIndexPattern2 = mock(Role.IndexPatterns.class);

        Pattern pattern1 = Pattern.create("test-*");
        Pattern pattern2 = Pattern.create("prod-*");

        when(roleIndexPattern1.getPattern()).thenReturn(pattern1);
        when(roleIndexPattern1.getPatternTemplates()).thenReturn(ImmutableList.empty());
        when(roleIndexPattern1.getDateMathExpressions()).thenReturn(ImmutableList.empty());

        when(roleIndexPattern2.getPattern()).thenReturn(pattern2);
        when(roleIndexPattern2.getPatternTemplates()).thenReturn(ImmutableList.empty());
        when(roleIndexPattern2.getDateMathExpressions()).thenReturn(ImmutableList.empty());

        IndexPattern.Builder builder = new IndexPattern.Builder();
        builder.add(roleIndexPattern1);
        builder.add(roleIndexPattern2);
        IndexPattern indexPattern = builder.build();

        assertThat(indexPattern != null, is(true));
    }

    @Test
    public void testBuilderEmptyBuild() {
        IndexPattern.Builder builder = new IndexPattern.Builder();
        IndexPattern indexPattern = builder.build();

        assertThat(indexPattern != null, is(true));
    }

    @Test
    public void testConstantPatternMatchesTakesPrecedence()
            throws PrivilegesEvaluationException, ConfigValidationException, ExpressionEvaluationException {
        Pattern constantPattern = Pattern.create("test-index");
        Role.IndexPatterns.IndexPatternTemplate template = mock(Role.IndexPatterns.IndexPatternTemplate.class);

        IndexPattern indexPattern = new IndexPattern(constantPattern, ImmutableList.of(template), ImmutableList.empty());

        assertThat(indexPattern.matches("test-index", mockUser, mockContext, mockMeter), is(true));
        verify(mockContext, never()).getRenderedPattern(any());
    }

    @Test
    public void testMultiplePatternTemplates() throws PrivilegesEvaluationException, ExpressionEvaluationException, ConfigValidationException {
        Pattern constantPattern = Pattern.create("constant-*");
        Pattern renderedPattern1 = Pattern.create("template1-*");
        Pattern renderedPattern2 = Pattern.create("template2-*");
        Pattern exclusion = Pattern.create("excluded");

        Role.IndexPatterns.IndexPatternTemplate template1 = mock(Role.IndexPatterns.IndexPatternTemplate.class);
        Role.IndexPatterns.IndexPatternTemplate template2 = mock(Role.IndexPatterns.IndexPatternTemplate.class);

        when(template1.getExclusions()).thenReturn(exclusion);

        when(mockContext.getRenderedPattern(any())).thenReturn(renderedPattern1).thenReturn(renderedPattern2);

        IndexPattern indexPattern = new IndexPattern(constantPattern, ImmutableList.of(template1, template2), ImmutableList.empty());

        assertThat(indexPattern.matches("template1-data", mockUser, mockContext, mockMeter), is(true));
    }

    @Test
    public void testMultipleDateMathExpressions() throws PrivilegesEvaluationException, ExpressionEvaluationException, ConfigValidationException {
        Pattern constantPattern = Pattern.create("other-*");
        Pattern dateMath1 = Pattern.create("logs-2025-*");
        Pattern dateMath2 = Pattern.create("logs-2024-*");
        Pattern exclusion = Pattern.create("excluded");

        Role.IndexPatterns.DateMathExpression expr1 = mock(Role.IndexPatterns.DateMathExpression.class);
        Role.IndexPatterns.DateMathExpression expr2 = mock(Role.IndexPatterns.DateMathExpression.class);

        when(expr1.getExclusions()).thenReturn(exclusion);

        when(mockContext.getRenderedDateMathExpression(any())).thenReturn(dateMath1).thenReturn(dateMath2);

        IndexPattern indexPattern = new IndexPattern(constantPattern, ImmutableList.empty(), ImmutableList.of(expr1, expr2));

        assertThat(indexPattern.matches("logs-2025-01", mockUser, mockContext, mockMeter), is(true));
    }
}