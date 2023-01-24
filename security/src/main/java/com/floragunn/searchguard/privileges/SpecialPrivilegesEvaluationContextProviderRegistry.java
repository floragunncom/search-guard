/*
 * Copyright 2023 floragunn GmbH
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
package com.floragunn.searchguard.privileges;

import com.floragunn.searchguard.user.User;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.ThreadContext;

public class SpecialPrivilegesEvaluationContextProviderRegistry implements SpecialPrivilegesEvaluationContextProvider {
    private static final Logger log = LogManager.getLogger(SpecialPrivilegesEvaluationContextProviderRegistry.class);

    private List<SpecialPrivilegesEvaluationContextProvider> providers = new ArrayList<>();

    public void add(SpecialPrivilegesEvaluationContextProvider provider) {
        this.providers.add(provider);
    }

    @Override
    public void provide(User user, ThreadContext threadContext, Consumer<SpecialPrivilegesEvaluationContext> onResult,
            Consumer<Exception> onFailure) {
        provide(this.providers.iterator(), user, threadContext, onResult, onFailure);
    }

    private void provide(Iterator<SpecialPrivilegesEvaluationContextProvider> iter, User user, ThreadContext threadContext,
            Consumer<SpecialPrivilegesEvaluationContext> onResult, Consumer<Exception> onFailure) {
        try {
            if (iter.hasNext()) {
                SpecialPrivilegesEvaluationContextProvider provider = iter.next();

                try {
                    provider.provide(user, threadContext, (result) -> {
                        if (result != null) {
                            onResult.accept(result);
                        } else {
                            provide(iter, user, threadContext, onResult, onFailure);
                        }
                    }, onFailure);
                } catch (Exception e) {
                    log.error("Error in " + provider, e);
                    onFailure.accept(e);
                }
            } else {
                onResult.accept(null);
            }
        } catch (Exception e) {
            log.error("Error in SpecialPrivilegesEvaluationContextProviderRegistry", e);
            onFailure.accept(e);
        } catch (Throwable t) {
            log.error("Error in SpecialPrivilegesEvaluationContextProviderRegistry", t);
            onFailure.accept(new RuntimeException(t));
        }
    }

    public SpecialPrivilegesEvaluationContext provide(User user, ThreadContext threadContext) {
        CompletableFuture<SpecialPrivilegesEvaluationContext> completableFuture = new CompletableFuture<>();

        provide(user, threadContext, completableFuture::complete, completableFuture::completeExceptionally);

        try {
            return completableFuture.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }
}
