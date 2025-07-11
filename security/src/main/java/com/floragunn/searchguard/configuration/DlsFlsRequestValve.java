/*
 * Copyright 2015-2022 floragunn GmbH
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

package com.floragunn.searchguard.configuration;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.actions.ResolvedIndices;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContext;
import com.floragunn.searchguard.user.User;

public interface DlsFlsRequestValve {

    /**
     * Invoked for calls intercepted by ActionFilter
     * 
     * @param request
     * @param listener
     * @return false to stop
     */
    boolean invoke(User user, ImmutableSet<String> mappedRoles, String action, ActionRequest request, ActionListener<?> listener,
            boolean localHasingEnabled, ResolvedIndices resolved, SpecialPrivilegesEvaluationContext specialPrivilegesEvaluationContext);

    void handleSearchContext(SearchContext context, ThreadPool threadPool, NamedXContentRegistry namedXContentRegistry);

    public static class NoopDlsFlsRequestValve implements DlsFlsRequestValve {

        @Override
        public boolean invoke(User user, ImmutableSet<String> mappedRoles, String action, ActionRequest request, ActionListener<?> listener,
                boolean localHasingEnabled, ResolvedIndices resolved, SpecialPrivilegesEvaluationContext specialPrivilegesEvaluationContext) {
            return true;
        }

        @Override
        public void handleSearchContext(SearchContext context, ThreadPool threadPool, NamedXContentRegistry namedXContentRegistry) {

        }
    }

}
