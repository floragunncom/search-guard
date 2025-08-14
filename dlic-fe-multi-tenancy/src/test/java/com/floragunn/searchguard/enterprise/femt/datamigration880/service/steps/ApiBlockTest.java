package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.junit.Test;


public class ApiBlockTest {
    @Test
    public void shouldUseEnum() {
        IndexMetadata.builder("abc"); // this line solves the problem
        var enumValue = IndexMetadata.APIBlock.READ_ONLY;
    }
}
