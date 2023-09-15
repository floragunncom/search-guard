package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

public enum StepExecutionStatus {

    OK(true),
    INDICES_NOT_FOUND_ERROR,
    UNEXPECTED_ERROR,
    STATUS_INDEX_ALREADY_EXISTS,
    CANNOT_CREATE_STATUS_DOCUMENT_ERROR,
    MIGRATION_ALREADY_IN_PROGRESS_ERROR,
    CANNOT_UPDATE_STATUS_DOCUMENT_LOCK_ERROR,
    GLOBAL_TENANT_NOT_FOUND_ERROR,
    MULTI_TENANCY_CONFIG_NOT_AVAILABLE_ERROR,
    MULTI_TENANCY_DISABLED_ERROR,
    CANNOT_RESOLVE_INDEX_BY_ALIAS,
    UNHEALTHY_INDICES_ERROR;


    private final boolean success;

    StepExecutionStatus(boolean success) {
        this.success = success;
    }

    StepExecutionStatus() {
        this(false);
    }

    public boolean isSuccess() {
        return success;
    }
}
