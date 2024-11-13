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
package com.floragunn.searchguard.enterprise.femt.tenants;

import com.floragunn.searchsupport.action.Action;
import com.floragunn.searchsupport.action.StandardRequests.EmptyRequest;
import com.floragunn.searchsupport.action.StandardResponse;
import org.elasticsearch.injection.guice.Inject;

import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public class MultitenancyActivationAction extends Action<EmptyRequest, StandardResponse> {

    public final static String NAME = "cluster:admin:searchguard:config/multitenancy/activate";
    public static final MultitenancyActivationAction INSTANCE = new MultitenancyActivationAction();

    public MultitenancyActivationAction() {
        super(NAME, EmptyRequest::new, StandardResponse::new);
    }

    public static class MultitenancyActivationHandler extends Handler<EmptyRequest, StandardResponse> {

        private final MultitenancyActivationService activationService;

        @Inject
        public MultitenancyActivationHandler(
            HandlerDependencies handlerDependencies,
            MultitenancyActivationService activationService) {
            super(INSTANCE, handlerDependencies);
            this.activationService = requireNonNull(activationService, "MT activation service is required");
        }

        @Override
        protected CompletableFuture<StandardResponse> doExecute(EmptyRequest request) {
            return supplyAsync(() -> activationService.activate());
        }
    }
}