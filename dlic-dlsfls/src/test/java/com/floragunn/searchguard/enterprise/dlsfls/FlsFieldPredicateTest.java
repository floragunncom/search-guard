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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

import org.junit.Test;

import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.enterprise.dlsfls.FlsFieldFilter.FlsFieldPredicate;
import com.floragunn.searchguard.enterprise.dlsfls.RoleBasedFieldAuthorization.FlsRule;
import com.floragunn.searchguard.test.TestSgConfig;

/**
 * Tests for FlsFieldFilter.FlsFieldPredicate
 */
public class FlsFieldPredicateTest {

    @Test
    public void shouldModifyHash() throws Exception {
        Role role = new TestSgConfig.Role("test_role") //
                .indexPermissions("*") //
                .fls("field1", "field2") //
                .on("*") //
                .toActualRole();

        FlsRule flsRule = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                role.getIndexPermissions().get(0));

        FlsFieldPredicate predicate = new FlsFieldPredicate(flsRule);

        String inputHash = "genuine_hash";
        String modifiedHash = predicate.modifyHash(inputHash);

        assertThat("Modified hash should not be null", modifiedHash, notNullValue());
        assertThat("Modified hash should start with 'sg_fls_'",
                modifiedHash, startsWith("sg_fls_"));
        assertThat("Modified hash should contain original hash",
                modifiedHash, endsWith(":" + inputHash));
        assertThat("Modified hash should contain rule hash",
                modifiedHash, matchesPattern("^sg_fls_-?\\d+:genuine_hash$"));
    }
}