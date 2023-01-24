/*
  * Copyright 2023 by floragunn GmbH - All rights reserved
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
