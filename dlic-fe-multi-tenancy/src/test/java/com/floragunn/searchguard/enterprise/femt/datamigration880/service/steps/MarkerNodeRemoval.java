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
package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;

class MarkerNodeRemoval {

    public static DocNode withoutMigrationMarker(DocNode actualMappings) throws DocumentParseException {
        DocNode extensionRemoved = DocNode.wrap(actualMappings.get("properties")).without("sg_data_migrated_to_8_8_0");
        return DocNode.parse(Format.JSON).from(actualMappings.with("properties", extensionRemoved).toJsonString());
    }
}
