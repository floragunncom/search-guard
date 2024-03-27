/*
 * Copyright 2023-2024 by floragunn GmbH - All rights reserved
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

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.configuration.ConcurrentConfigUpdateException;
import com.floragunn.searchguard.configuration.ConfigUpdateException;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.enterprise.femt.FeMultiTenancyConfig;
import com.floragunn.searchguard.enterprise.femt.FeMultiTenancyConfigurationProvider;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationConfig;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Optional;

import static com.floragunn.searchsupport.junit.ThrowableAssert.assertThatThrown;
import static java.time.ZoneOffset.UTC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EnableMultitenancyStepTest {

    private static final ZonedDateTime NOW = ZonedDateTime.of(LocalDateTime.of(2020, 1, 1, 1, 1), UTC);

    private final static Clock CLOCK = Clock.fixed(NOW.toInstant(), UTC);
    private DataMigrationContext migrationContext;
    @Mock
    private FeMultiTenancyConfigurationProvider configurationProvider;
    @Mock
    private ConfigurationRepository repository;
    @Captor
    private ArgumentCaptor<SgDynamicConfiguration<FeMultiTenancyConfig>> configCaptor;

    // under tests
    private EnableMultitenancyStep step;

    @Before
    public void setUp() {
        this.migrationContext = new DataMigrationContext(new MigrationConfig(false), CLOCK);
        this.step = new EnableMultitenancyStep(configurationProvider, repository);
    }

    @Test
    public void shouldEnableMultitenancyWhenMtConfigDoesNotExist()
        throws ConfigUpdateException, ConcurrentConfigUpdateException, ConfigValidationException {
        when(configurationProvider.getConfig()).thenReturn(Optional.empty());

        StepResult result = step.execute(migrationContext);

        assertThat(result.isSuccess(), equalTo(true));
        verify(repository).update(eq(FeMultiTenancyConfig.TYPE), configCaptor.capture(), isNull(), eq(false));
        SgDynamicConfiguration<FeMultiTenancyConfig> dynamicConfig = configCaptor.getValue();
        FeMultiTenancyConfig capturedConfig = dynamicConfig.getCEntry("default");
        assertThat(capturedConfig.isEnabled(), equalTo(true));
    }

    @Test
    public void shouldEnableMultitenancyWhenMtIsDisabled()
        throws ConfigUpdateException, ConcurrentConfigUpdateException, ConfigValidationException {
        when(configurationProvider.getConfig()).thenReturn(Optional.of(FeMultiTenancyConfig.DEFAULT.withEnabled(false)));

        StepResult result = step.execute(migrationContext);

        assertThat(result.isSuccess(), equalTo(true));
        verify(repository).update(eq(FeMultiTenancyConfig.TYPE), configCaptor.capture(), isNull(), eq(false));
        SgDynamicConfiguration<FeMultiTenancyConfig> dynamicConfig = configCaptor.getValue();
        FeMultiTenancyConfig capturedConfig = dynamicConfig.getCEntry("default");
        assertThat(capturedConfig.isEnabled(), equalTo(true));
    }

    @Test
    public void shouldNotUpdateConfigurationWhenMultitenancyIsAlreadyEnabled()
        throws ConfigUpdateException, ConcurrentConfigUpdateException, ConfigValidationException {
        when(configurationProvider.getConfig()).thenReturn(Optional.of(FeMultiTenancyConfig.DEFAULT.withEnabled(true)));

        StepResult result = step.execute(migrationContext);

        assertThat(result.isSuccess(), equalTo(true));
        verify(repository, never()).update(eq(FeMultiTenancyConfig.TYPE), any(), isNull(), eq(false));
    }

    @Test
    public void shouldConvertRepositoryExceptionToStepException()
        throws ConfigUpdateException, ConcurrentConfigUpdateException, ConfigValidationException {
        when(configurationProvider.getConfig()).thenReturn(Optional.of(FeMultiTenancyConfig.DEFAULT.withEnabled(false)));
        doThrow(ConcurrentConfigUpdateException.class)//
            .when(repository) //
            .update(eq(FeMultiTenancyConfig.TYPE), any(), isNull(), eq(false));

        StepException exception = (StepException) assertThatThrown(() -> step.execute(migrationContext), instanceOf(StepException.class));

        assertThat(exception.getStatus(), equalTo(StepExecutionStatus.CANNOT_ENABLE_MULTITENANCY));
    }

}