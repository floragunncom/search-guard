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
