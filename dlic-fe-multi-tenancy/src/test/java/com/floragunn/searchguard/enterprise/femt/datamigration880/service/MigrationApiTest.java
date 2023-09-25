package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.persistence.IndexMigrationStateRepository;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

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

    @BeforeClass
    public static void beforeClass() {
        ConfigurationRepository configRepository = cluster.getInjectable(ConfigurationRepository.class);
        SgDynamicConfiguration<Tenant> configuration = configRepository.getConfiguration(CType.TENANTS);
        // TODO handle creation index for global tenant
        Set<String> aliasNames = configuration.getCEntries()
            .keySet()
            .stream()
            .map(tenantName -> toInternalIndexName(".kibana", tenantName))
            .collect(Collectors.toSet());
        Client client = cluster.getInternalNodeClient();
        for(String alias : aliasNames) {
            String indexName = alias + "_8.7.0_001";
            String shortAlias = alias + "_8.7.0";
            CreateIndexRequest request = new CreateIndexRequest(indexName).alias(new Alias(alias)).alias(new Alias(shortAlias))
                .settings(Settings.builder().put("index.number_of_replicas", 0));
            client.admin().indices().create(request).actionGet();
        }
        // global tenant index
        client.admin()
            .indices() //
            .create(new CreateIndexRequest(".kibana_8.7.0_001") //
                .alias(new Alias(".kibana_8.7.0")) //
                .alias(new Alias(".kibana")) //
                .settings(Settings.builder().put("index.number_of_replicas", 0))) //
            .actionGet();
        // user tenant indices
        String[][] userIndicesAndAliases = new String[][] {
            //{"index name", "long alias", "short alias"}
            {".kibana_3292183_kirk_8.7.0_001", ".kibana_3292183_kirk_8.7.0", ".kibana_3292183_kirk" },// kirk
            {".kibana_-1091682490_lukasz_8.7.0_001", ".kibana_-1091682490_lukasz_8.7.0", ".kibana_-1091682490_lukasz"}, //lukasz
            {".kibana_739988528_ukasz_8.7.0_001", ".kibana_739988528_ukasz_8.7.0", ".kibana_739988528_ukasz"}, //łukasz
            {".kibana_-1091714203_luksz_8.7.0_001", ".kibana_-1091714203_luksz_8.7.0", ".kibana_-1091714203_luksz"}//luk@sz
        };
        for(String[] privateUserTenant : userIndicesAndAliases) {
            client.admin()
                .indices() //
                .create(new CreateIndexRequest(privateUserTenant[0]) //
                    .alias(new Alias(privateUserTenant[1])) //
                    .alias(new Alias(privateUserTenant[2])) //
                    .settings(Settings.builder().put("index.number_of_replicas", 0))) //
                .actionGet();
        }
    }

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
            DocNode body = DocNode.EMPTY;
            HttpResponse response = client.postJson("/_searchguard/config/fe_multi_tenancy/data_migration/8_8_0", body);

            log.info("Start migration response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));

        }
    }

    private static String toInternalIndexName(String prefix, String tenant) {
        if (tenant == null) {
            throw new ElasticsearchException("tenant must not be null here");
        }
        String tenantInfoPart = "_" + tenant.hashCode() + "_" + tenant.toLowerCase().replaceAll("[^a-z0-9]+", "");
        StringBuilder result = new StringBuilder(prefix).append(tenantInfoPart);
        return result.toString();
    }
}
