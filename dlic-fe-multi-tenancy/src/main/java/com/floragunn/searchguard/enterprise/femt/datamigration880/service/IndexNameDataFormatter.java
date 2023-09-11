package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

class IndexNameDataFormatter {
    private final static DateTimeFormatter INDEX_FORMATTER = DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss");

    public static String format(LocalDateTime localDateTime) {
        return INDEX_FORMATTER.format(localDateTime);
    }
}
