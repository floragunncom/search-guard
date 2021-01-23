package com.floragunn.searchguard.privileges;

import java.util.function.Consumer;

import org.elasticsearch.common.util.concurrent.ThreadContext;

import com.floragunn.searchguard.user.User;

@FunctionalInterface
public interface SpecialPrivilegesEvaluationContextProvider {
    void provide(User user, ThreadContext threadContext, Consumer<SpecialPrivilegesEvaluationContext> onResult, Consumer<Exception> onFailure);
}
