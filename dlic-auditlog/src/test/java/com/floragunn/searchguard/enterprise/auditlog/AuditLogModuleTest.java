package com.floragunn.searchguard.enterprise.auditlog;

import com.floragunn.searchguard.BaseDependencies;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.license.LicenseRepository;
import com.floragunn.searchguard.support.ConfigConstants;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AuditLogModuleTest {

    @Test
    public void shouldNotCreateAnyComponent_auditLogsAreDisabled() {
        Settings settings = Settings.builder().putNull(ConfigConstants.SEARCHGUARD_AUDIT_TYPE_DEFAULT).build();
        BaseDependencies baseDependencies = buildBaseDependencies(settings);
        AuditLogModule auditLogModule = new AuditLogModule();

        auditLogModule.createComponents(baseDependencies);

        assertThat(auditLogModule.getAuditLog(), nullValue());
        assertThat(auditLogModule.getDirectoryReaderWrappersForAllOperations(), empty());
        assertThat(auditLogModule.getIndexOperationListeners(), empty());
        assertThat(auditLogModule.getActionFilters(), empty());
    }

    @Test
    public void shouldCreateComponents_auditLogsAreEnabled() {
        Settings settings = Settings.builder().put(ConfigConstants.SEARCHGUARD_AUDIT_TYPE_DEFAULT, "something").build();
        BaseDependencies baseDependencies = buildBaseDependencies(settings);
        AuditLogModule auditLogModule = new AuditLogModule();

        auditLogModule.createComponents(baseDependencies);

        assertThat(auditLogModule.getAuditLog(), notNullValue());
        assertThat(auditLogModule.getDirectoryReaderWrappersForAllOperations(), hasSize(1));
        assertThat(auditLogModule.getIndexOperationListeners(), hasSize(1));
        assertThat(auditLogModule.getActionFilters(), hasSize(2));
    }

    private BaseDependencies buildBaseDependencies(Settings settings) {
        Environment environment = mock(Environment.class);
        when(environment.settings()).thenReturn(settings);
        ConfigurationRepository configurationRepository = mock(ConfigurationRepository.class);
        LicenseRepository licenseRepository = mock(LicenseRepository.class);
        return new BaseDependencies(
                settings, null,null,null,null,null,
                null,null, environment,null,null,null,
                configurationRepository, licenseRepository,null,null,null,
                null,null,null,null,null,null,
                null,null,null,null,null,null, null, null
        );
    }
}
