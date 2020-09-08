package com.floragunn.searchguard.privileges;

import org.elasticsearch.common.util.concurrent.ThreadContext;

import com.floragunn.searchguard.user.User;

@FunctionalInterface
public interface SpecialPrivilegesEvaluationContextProvider {
    SpecialPrivilegesEvaluationContext apply(User user, ThreadContext threadContext);
}
