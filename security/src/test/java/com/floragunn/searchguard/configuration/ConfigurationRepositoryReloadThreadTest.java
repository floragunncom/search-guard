/*
 * Copyright 2025 floragunn GmbH
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

/*
 * Adapted from the OpenSearch Security project (Apache-2.0):
 * https://github.com/opensearch-project/security/pull/5479
 *
 * Modifications Copyright OpenSearch Contributors. See GitHub history for details.
 */

package com.floragunn.searchguard.configuration;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.junit.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link ConfigurationRepository.ReloadThread}, the dedicated worker that serializes and coalesces
 * configuration reloads triggered by config update requests.
 */
public class ConfigurationRepositoryReloadThreadTest {

    static final Settings settings = Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "test_node").build();

    @Test
    public void singleRequest() {
        Set<CType<?>> requestedConfigTypes = Set.of(CType.INTERNALUSERS, CType.ROLES);
        AtomicInteger reloadCounter = new AtomicInteger(0);
        Set<CType<?>> reloadedConfigTypes = Collections.synchronizedSet(new HashSet<>());
        ConfigurationRepository.ReloadThread subject = new ConfigurationRepository.ReloadThread(settings, (configTypes, reason) -> {
            reloadCounter.incrementAndGet();
            reloadedConfigTypes.addAll(configTypes);
        });
        subject.start();
        subject.requestReload(requestedConfigTypes, "test", null);

        await().until(subject::isIdle);
        assertEquals("Exactly one reload should have been performed after the reload request", 1, reloadCounter.get());
        assertEquals("The reloaded config types match the requested config types", requestedConfigTypes, reloadedConfigTypes);
    }

    @Test
    public void twoRequestsBeforeStart() {
        AtomicInteger reloadCounter = new AtomicInteger(0);
        Set<CType<?>> reloadedConfigTypes = Collections.synchronizedSet(new HashSet<>());
        ConfigurationRepository.ReloadThread subject = new ConfigurationRepository.ReloadThread(settings, (configTypes, reason) -> {
            reloadCounter.incrementAndGet();
            reloadedConfigTypes.addAll(configTypes);
        });
        subject.requestReload(Set.of(CType.INTERNALUSERS), "test", null);
        subject.requestReload(Set.of(CType.ROLES), "test", null);
        subject.start();

        await().until(subject::isIdle);
        assertEquals("Exactly one reload should have been performed after the reload request", 1, reloadCounter.get());
        assertEquals("The reloaded config types match the requested config types", Set.of(CType.INTERNALUSERS, CType.ROLES),
                reloadedConfigTypes);
    }

    @Test
    public void oneQueuedRequest() {
        AtomicInteger reloadCounter = new AtomicInteger(0);
        // The following boolean allows us to synchronize between the reload code and the assertion for testing purposes. This helps to
        // avoid using Thread.sleep() calls.
        AtomicBoolean reloadContinueCondition = new AtomicBoolean(false);
        Set<CType<?>> reloadedConfigTypes = Collections.synchronizedSet(new HashSet<>());
        ConfigurationRepository.ReloadThread subject = new ConfigurationRepository.ReloadThread(settings, (configTypes, reason) -> {
            reloadCounter.incrementAndGet();
            reloadedConfigTypes.addAll(configTypes);
            await().until(reloadContinueCondition::get);
        });
        subject.start();
        subject.requestReload(Set.of(CType.INTERNALUSERS), "test", null);
        await().until(subject::queueIsEmpty);

        subject.requestReload(Set.of(CType.ROLES), "test", null);

        // Signal the reload function to finish
        reloadContinueCondition.set(true);

        await().until(subject::isIdle);
        assertEquals("Two reload requests have been performed now", 2, reloadCounter.get());
        assertEquals("The reloaded config types match the requested config types", Set.of(CType.INTERNALUSERS, CType.ROLES),
                reloadedConfigTypes);
    }

    @Test
    public void twoQueuedRequests() {
        AtomicInteger reloadCounter = new AtomicInteger(0);
        // The following boolean allows us to synchronize between the reload code and the assertion for testing purposes. This helps to
        // avoid using Thread.sleep() calls.
        AtomicBoolean reloadContinueCondition = new AtomicBoolean(false);
        Set<CType<?>> reloadedConfigTypes = Collections.synchronizedSet(new HashSet<>());
        ConfigurationRepository.ReloadThread subject = new ConfigurationRepository.ReloadThread(settings, (configTypes, reason) -> {
            reloadCounter.incrementAndGet();
            reloadedConfigTypes.addAll(configTypes);
            await().until(reloadContinueCondition::get);
        });
        subject.start();
        subject.requestReload(Set.of(CType.INTERNALUSERS), "test", null);
        await().until(subject::queueIsEmpty);

        subject.requestReload(Set.of(CType.ROLES), "test", null);
        subject.requestReload(Set.of(CType.ROLESMAPPING), "test", null);

        // Signal the reload function to finish
        reloadContinueCondition.set(true);

        await().until(subject::isIdle);
        assertEquals("Two reload requests have been performed now", 2, reloadCounter.get());
        assertEquals("The reloaded config types match the requested config types",
                Set.of(CType.INTERNALUSERS, CType.ROLES, CType.ROLESMAPPING), reloadedConfigTypes);
    }

    @Test
    public void twoQueuedRequestsWithoutTypeChange() {
        AtomicInteger reloadCounter = new AtomicInteger(0);
        // The following boolean allows us to synchronize between the reload code and the assertion for testing purposes. This helps to
        // avoid using Thread.sleep() calls.
        AtomicBoolean reloadContinueCondition = new AtomicBoolean(false);
        Set<CType<?>> reloadedConfigTypes = Collections.synchronizedSet(new HashSet<>());
        ConfigurationRepository.ReloadThread subject = new ConfigurationRepository.ReloadThread(settings, (configTypes, reason) -> {
            reloadCounter.incrementAndGet();
            reloadedConfigTypes.addAll(configTypes);
            await().until(reloadContinueCondition::get);
        });
        subject.start();
        subject.requestReload(Set.of(CType.INTERNALUSERS), "test", null);
        await().until(subject::queueIsEmpty);

        subject.requestReload(Set.of(CType.ROLES, CType.ROLESMAPPING), "test", null);
        subject.requestReload(Set.of(CType.ROLESMAPPING), "test", null);

        // Signal the reload function to finish
        reloadContinueCondition.set(true);

        await().until(subject::isIdle);
        assertEquals("Two reload requests have been performed now", 2, reloadCounter.get());
        assertEquals("The reloaded config types match the requested config types",
                Set.of(CType.INTERNALUSERS, CType.ROLES, CType.ROLESMAPPING), reloadedConfigTypes);
    }

    @Test
    public void threadContinuesDespiteException() {
        AtomicInteger reloadCounter = new AtomicInteger(0);
        Set<CType<?>> reloadedConfigTypes = Collections.synchronizedSet(new HashSet<>());
        ConfigurationRepository.ReloadThread subject = new ConfigurationRepository.ReloadThread(settings, (configTypes, reason) -> {
            reloadCounter.incrementAndGet();
            reloadedConfigTypes.addAll(configTypes);
            if (configTypes.contains(CType.AUTHC)) {
                // We use the config type AUTHC to request an exception for testing
                throw new RuntimeException("Throwing exception, as requested");
            }
        });
        subject.start();
        subject.requestReload(Set.of(CType.AUTHC), "test", null);
        await().until(subject::queueIsEmpty);

        subject.requestReload(Set.of(CType.ROLES), "test", null);

        await().until(subject::isIdle);
        assertEquals("Two reload requests have been performed now", 2, reloadCounter.get());
        assertEquals("The reloaded config types match the requested config types", Set.of(CType.AUTHC, CType.ROLES), reloadedConfigTypes);
    }

}
