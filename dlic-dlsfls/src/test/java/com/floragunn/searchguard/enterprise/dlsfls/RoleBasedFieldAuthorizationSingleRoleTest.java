/*
 * Copyright 2025 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */
package com.floragunn.searchguard.enterprise.dlsfls;

import org.junit.Test;

import java.util.List;

import static com.floragunn.searchguard.enterprise.dlsfls.RoleBasedFieldAuthorization.FlsRule.SingleRole.fieldItselfWithAllItsParent;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class RoleBasedFieldAuthorizationSingleRoleTest {

    @Test
    public void shouldReturnFieldAndAllParentFieldsForMultiLevelField() {
        List<String> result = fieldItselfWithAllItsParent("foo.bar.baz");
        assertThat(result, contains("foo.bar.baz", "foo.bar", "foo"));
    }

    @Test
    public void shouldReturnFieldAndAllParentFieldsForMultiLevelFieldCaseTwo() {
        List<String> result = fieldItselfWithAllItsParent("a.b.c.d.e");
        assertThat(result, contains("a.b.c.d.e", "a.b.c.d", "a.b.c", "a.b", "a"));
    }

    @Test
    public void shouldReturnFieldAndParentForSingleDotField() {
        List<String> result = fieldItselfWithAllItsParent("foo.bar");
        assertThat(result, contains("foo.bar", "foo"));
    }

    @Test
    public void shouldReturnFieldItselfWhenNoDotPresent() {
        List<String> result = fieldItselfWithAllItsParent("foobar");
        assertThat(result, contains("foobar"));
        assertThat(result, hasSize(1));
    }

    @Test
    public void shouldReturnEmptyStringWhenInputIsEmpty() {
        List<String> result = fieldItselfWithAllItsParent("");
        assertThat(result, contains(""));
        assertThat(result, hasSize(1));
    }
}

