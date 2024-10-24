/*
 * Copyright 2024 floragunn GmbH
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

package com.floragunn.searchsupport.action;

import org.elasticsearch.action.support.IndicesOptions;

public class IndicesOptionsSupport {

    public static final IndicesOptions EXACT = new IndicesOptions(//
            new IndicesOptions.ConcreteTargetOptions(false),
            IndicesOptions.WildcardOptions.builder().resolveAliases(true).matchClosed(false).includeHidden(false).allowEmptyExpressions(false)
                    .matchOpen(false).build(),
            IndicesOptions.GatekeeperOptions.builder().allowClosedIndices(true).allowAliasToMultipleIndices(true).ignoreThrottled(false).build());

    public static IndicesOptions allowNoIndices(IndicesOptions indicesOptions) {
        if (indicesOptions.allowNoIndices()) {
            return indicesOptions;
        } else {
            return IndicesOptions.builder(indicesOptions)
                    .wildcardOptions(IndicesOptions.WildcardOptions.builder(indicesOptions.wildcardOptions()).allowEmptyExpressions(true).build())
                    .build();
        }
    }

    public static IndicesOptions ignoreUnavailable(IndicesOptions indicesOptions) {
        if (indicesOptions.ignoreUnavailable()) {
            return indicesOptions;
        } else {
            return IndicesOptions.builder(indicesOptions).concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
                    .build();
        }
    }

}
