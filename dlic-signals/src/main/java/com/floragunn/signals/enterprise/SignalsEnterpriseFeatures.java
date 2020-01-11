package com.floragunn.signals.enterprise;

import com.floragunn.signals.accounts.Account;
import com.floragunn.signals.enterprise.watch.action.handlers.jira.JiraAccount;
import com.floragunn.signals.enterprise.watch.action.handlers.jira.JiraAction;
import com.floragunn.signals.enterprise.watch.action.handlers.pagerduty.PagerDutyAccount;
import com.floragunn.signals.enterprise.watch.action.handlers.pagerduty.PagerDutyAction;
import com.floragunn.signals.watch.action.handlers.ActionHandler;

public class SignalsEnterpriseFeatures {
    public static void init() {
        ActionHandler.factoryRegistry.add(new JiraAction.Factory(), new PagerDutyAction.Factory());
        Account.factoryRegistry.add(new JiraAccount.Factory(), new PagerDutyAccount.Factory());
    }
}
