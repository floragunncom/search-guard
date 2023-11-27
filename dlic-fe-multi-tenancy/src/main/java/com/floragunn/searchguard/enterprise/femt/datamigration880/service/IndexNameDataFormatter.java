/*
 * Copyright 2023 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */
package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

public class IndexNameDataFormatter {
    private final static DateTimeFormatter INDEX_FORMATTER = DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss");

    public static String format(LocalDateTime localDateTime) {
        return INDEX_FORMATTER.format(localDateTime);
    }

    public static LocalDateTime parse(String dateString) {
        TemporalAccessor temporalAccessor = INDEX_FORMATTER.parse(dateString);
        return LocalDateTime.from(temporalAccessor);
    }
}
