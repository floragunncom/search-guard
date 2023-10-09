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

import com.floragunn.fluent.collections.ImmutableMap;

interface MappingTypes {

    ImmutableMap<String, Object> MAPPING_KEYWORD = ImmutableMap.of("type", "keyword");
    ImmutableMap<String, String> MAPPING_DATE = ImmutableMap.of("type", "date");
    ImmutableMap<String, String> MAPPING_LONG = ImmutableMap.of("type", "long");
    ImmutableMap<String, Object> MAPPING_KEYWORD_IGNORE_ABOVE = MAPPING_KEYWORD.with("ignore_above", 250);
    ImmutableMap<String, Object> MAPPING_TEXT_WITH_KEYWORD =  ImmutableMap.of("type", "text", "fields", ImmutableMap.of("keyword", MAPPING_KEYWORD_IGNORE_ABOVE));
}
