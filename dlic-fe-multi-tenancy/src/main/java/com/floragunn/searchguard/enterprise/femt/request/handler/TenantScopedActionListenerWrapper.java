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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;

import java.util.function.Consumer;
import java.util.function.Function;

class TenantScopedActionListenerWrapper<T> implements ActionListener<T> {

    private static final Logger LOG = LogManager.getLogger(TenantScopedActionListenerWrapper.class);

    private final ActionListener<?> delegate;
    private final Consumer<T> onResponse;
    private final Function<T, T> responseMapper;
    private final Consumer<Exception> onFailure;

    protected TenantScopedActionListenerWrapper(ActionListener<?> delegate, Consumer<T> onResponse, Function<T, T> responseMapper, Consumer<Exception> onFailure) {
        this.delegate = delegate;
        this.onResponse = onResponse;
        this.responseMapper = responseMapper;
        this.onFailure = onFailure.andThen(delegate::onFailure);
    }

    @Override
    public void onResponse(T response) {
        try {
            onResponse.accept(response);
            @SuppressWarnings("unchecked")
            ActionListener<T> mappedDelegate = (ActionListener<T>) delegate;
            mappedDelegate.onResponse(responseMapper.apply(response));
        } catch (Exception e) {
            LOG.error("An error occurred while handling {} response", response.getClass().getName(), e);
            delegate.onFailure(e);
        }

    }

    @Override
    public void onFailure(Exception e) {
        onFailure.accept(e);
    }
}
