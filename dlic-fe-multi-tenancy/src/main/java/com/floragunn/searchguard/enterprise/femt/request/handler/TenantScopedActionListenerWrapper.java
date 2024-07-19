/*
 * Copyright 2024 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */

package com.floragunn.searchguard.enterprise.femt.request.handler;

import com.floragunn.searchguard.enterprise.femt.request.mapper.Unscoper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;

import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

class TenantScopedActionListenerWrapper<T extends ActionResponse> implements ActionListener<T> {

    private static final Logger LOG = LogManager.getLogger(TenantScopedActionListenerWrapper.class);

    private final ActionListener<T> delegate;
    private final Unscoper<T> unscoper;
    private final StoredContext contextToRestore;
    private final Consumer<T> finalActionForResponse;

    protected TenantScopedActionListenerWrapper(ActionListener<?> delegate, StoredContext contextToRestore, Unscoper<T> unscoper,
        Consumer<T> finalActionForUnscopedResponse) {
        this.delegate = (ActionListener<T>) requireNonNull(delegate, "Action listener is required");
        this.unscoper = requireNonNull(unscoper, "Unscoper is required");
        this.contextToRestore = requireNonNull(contextToRestore, "Thread context is required");
        this.finalActionForResponse = requireNonNull(finalActionForUnscopedResponse, "Final action on response is required");
    }

    protected TenantScopedActionListenerWrapper(ActionListener<?> delegate, StoredContext contextToRestore, Unscoper<T> unscoper) {
        this(delegate, contextToRestore, unscoper, unscopedResponse -> LOG.debug("Nothing to do on response {}.", unscopedResponse));
    }

    @Override
    public void onResponse(T response) {
        try {
            contextToRestore.restore();
            T unscopedResponse = unscoper.unscopeResponse(response);
            try {
                delegate.onResponse(unscopedResponse);
            } finally {
                finalActionForResponse.accept(unscopedResponse);
            }
        } catch (Exception e) {
            LOG.error("An error occurred while handling {} response", response.getClass().getName(), e);
            delegate.onFailure(e);
        }

    }

    @Override
    public void onFailure(Exception e) {
        LOG.error("Error occurred when handling response in the tenant scope.", e);
        contextToRestore.restore();
        delegate.onFailure(e);
    }
}
