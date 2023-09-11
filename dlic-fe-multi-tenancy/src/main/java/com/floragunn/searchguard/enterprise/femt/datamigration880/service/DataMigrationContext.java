package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class DataMigrationContext {

    private final static AtomicInteger instanceCounter = new AtomicInteger(0);
    private final LocalDateTime startTime;
    private final String migrationId;

    DataMigrationContext(Clock clock) {
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
        return "backup_fe_migration_to_8_8_0_" + IndexNameDataFormatter.format(startTime);
    }

    public String getMigrationId() {
        return migrationId;
    }
}
