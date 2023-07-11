package com.floragunn.signals.actions.watch.generic.service;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.signals.actions.watch.generic.service.persistence.WatchParametersData;
import com.floragunn.signals.actions.watch.generic.service.persistence.WatchParametersRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.Optional;

public class WatchInstanceParameterLoader {

    private static final Logger log = LogManager.getLogger(WatchInstanceParameterLoader.class);

    private final String tenantId;

    private final WatchParametersRepository repository;

    public WatchInstanceParameterLoader(String tenantId, WatchParametersRepository repository) {
        this.tenantId = Objects.requireNonNull(tenantId, "Tenant id is required");
        this.repository = Objects.requireNonNull(repository, "Watch parameter repository is required");
    }

    public ImmutableList<WatchParametersData> findParameters(String watchId) {
        Objects.requireNonNull(watchId, "Watch id is required");
        log.debug("Loading watch parameters for watch '{}' and tenant '{}'.", watchId, tenantId);
        return repository.findByWatchId(tenantId, watchId);
    }

    public Optional<WatchParametersData> findOne(String watchId, String instanceId) {
        Objects.requireNonNull(watchId, "Watch id is required");
        Objects.requireNonNull(instanceId, "Generic watch instance id is required");
        return repository.findOneById(tenantId, watchId, instanceId);
    }
}
