package com.floragunn.signals.accounts;

import com.floragunn.signals.watch.action.handlers.email.EmailAccount;
import com.floragunn.signals.watch.action.handlers.jira.JiraAccount;
import com.floragunn.signals.watch.action.handlers.pagerduty.PagerDutyAccount;
import com.floragunn.signals.watch.action.handlers.slack.SlackAccount;

public enum AccountType {

    EMAIL(EmailAccount.class), SLACK(SlackAccount.class), PAGERDUTY(PagerDutyAccount.class), JIRA(JiraAccount.class);

    private Class<? extends Account> destinationImplClass;
    private String prefix;

    private AccountType(Class<? extends Account> destinationImplClass) {
        this.destinationImplClass = destinationImplClass;
        this.prefix = this.name().toLowerCase();
    }

    public Class<? extends Account> getImplClass() {
        return destinationImplClass;
    }

    public String getPrefix() {
        return prefix;
    }

    public static AccountType getByClass(Class<?> clazz) {
        if (clazz == null) {
            return null;
        }

        for (AccountType accountType : values()) {
            if (clazz.getName().equals(accountType.destinationImplClass.getName())) {
                return accountType;
            }
        }

        return null;
    }

    public static AccountType getByName(String name) {
        for (AccountType accountType : values()) {
            if (accountType.name().equalsIgnoreCase(name)) {
                return accountType;
            }
        }

        return null;
    }

}
