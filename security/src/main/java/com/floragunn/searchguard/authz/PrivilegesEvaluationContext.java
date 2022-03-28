/*
 * Copyright 2021-2022 floragunn GmbH
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

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;

import com.floragunn.codova.config.templates.ExpressionEvaluationException;
import com.floragunn.codova.config.templates.Template;
import com.floragunn.searchguard.support.Pattern;
import com.floragunn.searchguard.user.User;

public class PrivilegesEvaluationContext {
    private boolean resolveLocalAll = true;
    private final User user;
    private final IndexNameExpressionResolver resolver;
    private final ClusterService clusterService;
    private final Map<Template<Pattern>, Pattern> renderedPatternTemplateCache = new HashMap<>();

    PrivilegesEvaluationContext(User user, IndexNameExpressionResolver resolver, ClusterService clusterService) {
        this.user = user;
        this.resolver = resolver;
        this.clusterService = clusterService;
    }

    public User getUser() {
        return user;
    }

    public IndexNameExpressionResolver getResolver() {
        return resolver;
    }

    public ClusterService getClusterService() {
        return clusterService;
    }

    public boolean isResolveLocalAll() {
        return resolveLocalAll;
    }

    public void setResolveLocalAll(boolean resolveLocalAll) {
        this.resolveLocalAll = resolveLocalAll;
    }
    
    public Pattern getRenderedPattern(Template<Pattern> template) throws ExpressionEvaluationException {
        Pattern pattern = this.renderedPatternTemplateCache.get(template);
        
        if (pattern == null) {
            pattern = template.render(user);
            this.renderedPatternTemplateCache.put(template, pattern);            
        }
        
        return pattern;
    }
}
