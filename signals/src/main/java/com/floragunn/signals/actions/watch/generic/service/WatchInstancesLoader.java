package com.floragunn.signals.actions.watch.generic.service;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.signals.actions.watch.generic.service.persistence.WatchInstanceData;
import com.floragunn.signals.actions.watch.generic.service.persistence.WatchInstancesRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.Optional;

public class WatchInstancesLoader {

    private static final Logger log = LogManager.getLogger(WatchInstancesLoader.class);

    private final String tenantId;

    private final WatchInstancesRepository repository;

    public WatchInstancesLoader(String tenantId, WatchInstancesRepository repository) {
        this.tenantId = Objects.requireNonNull(tenantId, "Tenant id is required");
        this.repository = Objects.requireNonNull(repository, "Watch parameter repository is required");
    }

    public ImmutableList<WatchInstanceData> findInstances(String watchId) {
        Objects.requireNonNull(watchId, "Watch id is required");
        log.debug("Loading watch parameters for watch '{}' and tenant '{}'.", watchId, tenantId);
        return repository.findByWatchId(tenantId, watchId);
    }

    public Optional<WatchInstanceData> findOne(String watchId, String instanceId) {
        Objects.requireNonNull(watchId, "Watch id is required");
        Objects.requireNonNull(instanceId, "Generic watch instance id is required");
        return repository.findOneById(tenantId, watchId, instanceId);
    }
}
