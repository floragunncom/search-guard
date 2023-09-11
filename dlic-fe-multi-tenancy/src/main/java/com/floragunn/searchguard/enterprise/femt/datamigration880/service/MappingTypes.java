package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import com.floragunn.fluent.collections.ImmutableMap;

interface MappingTypes {

    ImmutableMap<String, Object> MAPPING_KEYWORD = ImmutableMap.of("type", "keyword");
    ImmutableMap<String, String> MAPPING_DATE = ImmutableMap.of("type", "date");
    ImmutableMap<String, String> MAPPING_LONG = ImmutableMap.of("type", "long");
    ImmutableMap<String, Object> MAPPING_KEYWORD_IGNORE_ABOVE = MAPPING_KEYWORD.with("ignore_above", 250);
    ImmutableMap<String, Object> MAPPING_TEXT_WITH_KEYWORD =  ImmutableMap.of("type", "text", "fields", ImmutableMap.of("keyword", MAPPING_KEYWORD_IGNORE_ABOVE));
}
