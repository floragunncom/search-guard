/*
 * Copyright 2026 floragunn GmbH
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

package com.floragunn.signals.accounts;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.Test;

public class AccountTest {

    @Test
    public void scopedIdUsesTypeAndIdForGlobalAccounts() {
        assertThat(Account.scopedId(null, "email", "account_1"), equalTo("email/account_1"));
    }

    @Test
    public void scopedIdPrefixesTenantForTenantAccounts() {
        assertThat(Account.scopedId("tenant_1", "email", "account_1"), equalTo("tenant_1/email/account_1"));
    }
}

