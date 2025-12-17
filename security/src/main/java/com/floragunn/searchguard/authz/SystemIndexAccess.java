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

package com.floragunn.searchguard.authz;

import java.util.function.Predicate;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.indices.SystemIndices.SystemIndexAccessLevel;

import com.floragunn.searchsupport.meta.Meta;

public class SystemIndexAccess {

    public final static SystemIndexAccess DISALLOWED = new SystemIndexAccess(false, (i) -> false);

    private final boolean allowed;
    private final Predicate<String> indexNamePredicate;

    SystemIndexAccess(boolean allowed, Predicate<String> indexNamePredicate) {
        this.allowed = allowed;
        this.indexNamePredicate = indexNamePredicate;
    }

    public boolean isNotAllowed() {
        return !allowed;
    }

    public boolean isAllowed(Meta.IndexLikeObject index) {
        return indexNamePredicate.test(index.nameForIndexPatternMatching());
    }

    public boolean isAllowed(String index) {
        return indexNamePredicate.test(index);
    }

    public static SystemIndexAccess get(IndexNameExpressionResolver indexNameExpressionResolver) {
        SystemIndexAccessLevel level = indexNameExpressionResolver.getSystemIndexAccessLevel();

        if (level == SystemIndexAccessLevel.NONE) {
            return DISALLOWED;
        } else {
            return new SystemIndexAccess(true, (i) -> indexNameExpressionResolver.getSystemIndexAccessPredicate().test(i));
        }
    }

}
