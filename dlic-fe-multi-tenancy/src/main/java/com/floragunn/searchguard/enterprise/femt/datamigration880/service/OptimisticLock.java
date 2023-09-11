package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

public record OptimisticLock(long primaryTerm, long seqNo) {
}
