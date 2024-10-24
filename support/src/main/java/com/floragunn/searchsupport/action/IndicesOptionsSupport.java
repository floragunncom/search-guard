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

import java.util.EnumSet;

import org.elasticsearch.action.support.IndicesOptions;

public class IndicesOptionsSupport {

    public static final IndicesOptions EXACT  = new IndicesOptions(EnumSet.noneOf(IndicesOptions.Option.class),
            EnumSet.noneOf(IndicesOptions.WildcardStates.class));

    public static IndicesOptions allowNoIndices(IndicesOptions indicesOptions) {
        if (indicesOptions.allowNoIndices()) {
            return indicesOptions;
        } else {
            EnumSet<IndicesOptions.Option> newOptions = indicesOptions.options().clone();
            newOptions.add(IndicesOptions.Option.ALLOW_NO_INDICES);
            return new IndicesOptions(newOptions, indicesOptions.expandWildcards());
        }
    }

    public static IndicesOptions ignoreUnavailable(IndicesOptions indicesOptions) {
        if (indicesOptions.ignoreUnavailable()) {
            return indicesOptions;
        } else {
            EnumSet<IndicesOptions.Option> newOptions = indicesOptions.options().clone();
            newOptions.add(IndicesOptions.Option.IGNORE_UNAVAILABLE);
            return new IndicesOptions(newOptions, indicesOptions.expandWildcards());
        }
    }

}
