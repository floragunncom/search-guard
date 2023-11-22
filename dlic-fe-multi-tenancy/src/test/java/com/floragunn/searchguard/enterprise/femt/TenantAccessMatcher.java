package com.floragunn.searchguard.enterprise.femt;

import com.floragunn.searchguard.test.GenericRestClient;
import java.util.EnumSet;
import org.hamcrest.Matcher;

public class TenantAccessMatcher {

    private TenantAccessMatcher() {

    }

    public static Matcher<GenericRestClient> canPerformFollowingActions(EnumSet<Action> allowedActions) {
        return new TenantsActionsMatcher(allowedActions);
    }

    public enum Action {
        CREATE_DOCUMENT, UPDATE_DOCUMENT, UPDATE_INDEX, DELETE_INDEX
    }
}