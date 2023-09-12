package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.searchguard.enterprise.femt.MultiTenancyConfigurationProvider;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.ExecutionStatus;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.ExecutionStatus.FAILURE;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.INDICES_NOT_FOUND_ERROR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class MigrationStepTest {

    private static final Logger log = LogManager.getLogger(MigrationStepTest.class);

    private static final ZonedDateTime NOW = ZonedDateTime.of(LocalDateTime.of(2000, 1, 1, 1, 1), ZoneOffset.UTC);

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder()
        .sslEnabled()
        .resources("multitenancy")
        .enterpriseModulesEnabled()
        .build();

    private Clock clock = Clock.fixed(NOW.toInstant(), ZoneOffset.UTC);;

    @Test
    public void shouldReportErrorWhenIndexForMigrationAreNotFound() {
        MultiTenancyConfigurationProvider configurationProvider = cluster.getInjectable(MultiTenancyConfigurationProvider.class);
        Client client = cluster.getInternalNodeClient();
        PopulateTenantsStep populateTenantsStep = new PopulateTenantsStep(configurationProvider, PrivilegedConfigClient.adapt(client));
        DataMigrationContext context = new DataMigrationContext(clock);

        StepResult result = populateTenantsStep.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.status(), equalTo(INDICES_NOT_FOUND_ERROR));
    }

}