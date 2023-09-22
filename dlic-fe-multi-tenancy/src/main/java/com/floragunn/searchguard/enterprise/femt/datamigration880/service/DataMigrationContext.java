package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import com.floragunn.fluent.collections.ImmutableList;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class DataMigrationContext {

    private final static AtomicInteger instanceCounter = new AtomicInteger(0);
    public static final String BACKUP_INDEX_NAME_PREFIX = "backup_fe_migration_to_8_8_0_";

    private final MigrationConfig config;
    private final LocalDateTime startTime;
    private final String migrationId;
    private ImmutableList<TenantIndex> tenantIndices;
    private ImmutableList<String> backupIndices;

    public DataMigrationContext(MigrationConfig config, Clock clock) {
        this.config = Objects.requireNonNull(config, "Migration config is required");
        this.startTime = LocalDateTime.now(clock);
        int instanceNumber = instanceCounter.incrementAndGet();
        String time = IndexNameDataFormatter.format(startTime);
        this.migrationId = "number-" + instanceNumber + "-" + time + "-" + UUID.randomUUID();
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public String getTempIndexName() {
        return "data_migration_temp_fe_" + IndexNameDataFormatter.format(startTime);
    }

    public String getBackupIndexName() {
        return BACKUP_INDEX_NAME_PREFIX + IndexNameDataFormatter.format(startTime);
    }

    public String getMigrationId() {
        return migrationId;
    }

    public ImmutableList<TenantIndex> getTenantIndices() {
        return tenantIndices;
    }

    public void setTenantIndices(ImmutableList<TenantIndex> tenantIndices) {
        Objects.requireNonNull(tenantIndices, "Tenants list must not be null");
        this.tenantIndices = tenantIndices;
    }

    public ImmutableList<String> getDataIndicesNames() {
        return Optional.ofNullable(tenantIndices).orElse(ImmutableList.empty()).map(TenantIndex::indexName);
    }

    public String getGlobalTenantIndexName() {
        return tenantIndices.stream() //
            .filter(TenantIndex::belongsToGlobalTenant) //
            .map(TenantIndex::indexName) //
            .findAny() //
            .orElseThrow(() -> new IllegalStateException("Global tenant not found!"));
    }

    public boolean areYellowDataIndicesAllowed() {
        return config.allowYellowIndices();
    }

    public ImmutableList<String> getBackupIndices() {
        return backupIndices;
    }

    public void setBackupIndices(ImmutableList<String> backupIndices) {
        this.backupIndices = backupIndices;
    }

    public Optional<String> getNewestExistingBackupIndex() {
        return Optional.ofNullable(backupIndices).filter(list -> !list.isEmpty()).map(list -> list.get(0));
    }
}
