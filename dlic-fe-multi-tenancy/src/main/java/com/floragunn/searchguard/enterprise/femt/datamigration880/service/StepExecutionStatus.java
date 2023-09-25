package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

public enum StepExecutionStatus {

    OK(true),
    /**
     * Rollback step executed successfully
     */
    ROLLBACK(true),
    INDICES_NOT_FOUND_ERROR,
    UNEXPECTED_ERROR,
    STATUS_INDEX_ALREADY_EXISTS_ERROR,
    CANNOT_CREATE_STATUS_DOCUMENT_ERROR,
    MIGRATION_ALREADY_IN_PROGRESS_ERROR,
    CANNOT_UPDATE_STATUS_DOCUMENT_LOCK_ERROR,
    GLOBAL_TENANT_NOT_FOUND_ERROR,
    MULTI_TENANCY_CONFIG_NOT_AVAILABLE_ERROR,
    MULTI_TENANCY_DISABLED_ERROR,
    CANNOT_RESOLVE_INDEX_BY_ALIAS_ERROR,
    UNHEALTHY_INDICES_ERROR,
    DATA_INDICES_LOCKED_ERROR,
    INVALID_BACKUP_INDEX_NAME_ERROR,
    INVALID_DATE_IN_BACKUP_INDEX_NAME_ERROR,
    WRITE_BLOCK_ERROR,
    WRITE_UNBLOCK_ERROR,
    CANNOT_RETRIEVE_INDICES_STATE_ERROR,
    NO_GLOBAL_TENANT_SETTINGS_ERROR,
    CANNOT_LOAD_INDEX_MAPPINGS_ERROR,
    CANNOT_CREATE_INDEX_ERROR;


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
