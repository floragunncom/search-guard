package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import com.floragunn.searchguard.enterprise.femt.datamigration880.service.persistence.IndexMigrationStateRepository;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.internal.Client;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;

import static org.apache.http.HttpStatus.SC_OK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class MigrationApiTest {

    private static final Logger log = LogManager.getLogger(MigrationApiTest.class);

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder()
        .sslEnabled()
        .resources("multitenancy")
        .enterpriseModulesEnabled()
        .build();

    @Before
    public void before() {
        Client client = cluster.getInternalNodeClient();
        IndexMigrationStateRepository repository = new IndexMigrationStateRepository(PrivilegedConfigClient.adapt(client));
        if(repository.isIndexCreated()) {
            client.admin().indices().delete(new DeleteIndexRequest(".sg_data_migration_state"));
        }
    }

    @Test
    public void shouldStartMigrationProcess() throws Exception {
        try (GenericRestClient client = cluster.createGenericAdminRestClient(Collections.emptyList())) {
            HttpResponse response = client.post("/_searchguard/config/fe_multi_tenancy/data_migration/8_8_0");

            log.info("Start migration response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
        }
    }
}
