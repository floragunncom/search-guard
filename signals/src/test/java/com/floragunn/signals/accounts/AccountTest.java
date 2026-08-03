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

