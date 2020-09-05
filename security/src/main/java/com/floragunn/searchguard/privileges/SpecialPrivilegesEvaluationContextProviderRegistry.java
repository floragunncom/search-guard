package com.floragunn.searchguard.privileges;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.util.concurrent.ThreadContext;

import com.floragunn.searchguard.user.User;

public class SpecialPrivilegesEvaluationContextProviderRegistry implements SpecialPrivilegesEvaluationContextProvider {

    private List<SpecialPrivilegesEvaluationContextProvider> providers = new ArrayList<>();

    public void add(SpecialPrivilegesEvaluationContextProvider provider) {
        this.providers.add(provider);
    }

    @Override
    public SpecialPrivilegesEvaluationContext apply(User user, ThreadContext threadContext) {
        for (SpecialPrivilegesEvaluationContextProvider provider : providers) {
            SpecialPrivilegesEvaluationContext context = provider.apply(user, threadContext);

            if (context != null) {
                return context;
            }
        }

        return null;
    }

}
