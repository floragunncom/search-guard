/*
 * Copyright 2021 floragunn GmbH
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

package com.floragunn.searchguard.filter;

import java.util.Iterator;
import java.util.List;

import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.tasks.Task;

public class ExtendedActionFilterChain<Request extends ActionRequest, Response extends ActionResponse>
        implements ActionFilterChain<Request, Response> {
    private final List<ActionFilter> additionalFilters;
    private final ActionFilterChain<Request, Response> originalChain;
    private final Iterator<ActionFilter> additionalFiltersIter;

    public ExtendedActionFilterChain(List<ActionFilter> additionalFilters, ActionFilterChain<Request, Response> originalChain) {
        this.additionalFilters = additionalFilters;
        this.additionalFiltersIter = additionalFilters.iterator();
        this.originalChain = originalChain;
    }

    @Override
    public void proceed(Task task, String action, Request request, ActionListener<Response> listener) {
        if (additionalFiltersIter.hasNext()) {
            ActionFilter filter = additionalFiltersIter.next();
            filter.apply(task, action, request, listener, this);
        } else {
            originalChain.proceed(task, action, request, listener);
        }

    }

    @Override
    public String toString() {
        return "ExtendedActionFilterChain [additionalFilters=" + additionalFilters + ", originalChain=" + originalChain + "]";
    }
}
