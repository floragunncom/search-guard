package com.floragunn.searchguard.enterprise.dlsfls;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class DlsFlsProcessedConfigTest {

    private static final ValidationError VALIDATION_ERROR_1 = new ValidationError("attribute-1", "error-message-1");
    private static final ValidationError VALIDATION_ERROR_2 = new ValidationError("attribute-2", "error-message-2");
    private static final String INVALID_ROLE_1 = "my-invalid-role-1";
    private static final String INVALID_ROLE_2 = "my-invalid-role-2";
    private static final String INVALID_MAPPING_1 = "my-invalid-mapping-1";
    private static final String INVALID_MAPPING_2 = "my-invalid-mapping-2";

    // under the tests
    private DlsFlsProcessedConfig config;

    @Mock
    private DlsFlsConfig dlsDlsConfig;
    @Mock
    private ValidationErrors rolesMapingdValidationErrors;
    @Mock
    private ValidationErrors rolesValidationErrors;
    @Mock
    private RoleBasedFieldMasking fieldMasking;
    @Mock
    private RoleBasedFieldAuthorization fieldAuthorization;
    @Mock
    private RoleBasedDocumentAuthorization documentAuthorization;

    @Test
    public void shouldSupportNullValidationErrors() {
        this.config = new DlsFlsProcessedConfig(dlsDlsConfig, documentAuthorization, fieldAuthorization, fieldMasking, null, null, null);

        assertThat(config.containsValidationError(), equalTo(false));
        assertThat(config.getValidationErrorDescription(), isEmptyOrNullString());
    }

    @Test
    public void shouldNotContainValidationErrors() {
        this.config = new DlsFlsProcessedConfig(dlsDlsConfig, documentAuthorization, fieldAuthorization, fieldMasking, null, rolesValidationErrors,
                rolesMapingdValidationErrors);

        assertThat(config.containsValidationError(), equalTo(false));
        assertThat(config.getValidationErrorDescription(), isEmptyOrNullString());
    }

    @Test
    public void shouldReturnRoleValidationErrorForRoles() {
        when(rolesValidationErrors.hasErrors()).thenReturn(true);
        when(rolesValidationErrors.getErrors()).thenReturn(ImmutableMap.of(INVALID_ROLE_1, singleton(VALIDATION_ERROR_1)));

        this.config = new DlsFlsProcessedConfig(dlsDlsConfig, documentAuthorization, fieldAuthorization, fieldMasking,null,
            rolesValidationErrors, rolesMapingdValidationErrors);

        assertThat(config.containsValidationError(), equalTo(true));
        assertThat(config.getUniqueValidationErrorToken(), not(isEmptyString()));
        assertThat(config.getValidationErrorDescription(), containsString(config.getUniqueValidationErrorToken()));
        assertThat(config.getValidationErrorDescription(), containsString(INVALID_ROLE_1));
        assertThat(config.getValidationErrorDescription(), containsString(VALIDATION_ERROR_1.getMessage()));
    }

    @Test
    public void shouldReturnMultipleRoleValidationError() {
        when(rolesValidationErrors.hasErrors()).thenReturn(true);
        when(rolesValidationErrors.getErrors()).thenReturn(ImmutableMap.of(INVALID_ROLE_1, singleton(VALIDATION_ERROR_1),//
            INVALID_ROLE_2, singleton(VALIDATION_ERROR_2)));

        this.config = new DlsFlsProcessedConfig(dlsDlsConfig, documentAuthorization, fieldAuthorization, fieldMasking,null,
            rolesValidationErrors, rolesMapingdValidationErrors);

        assertThat(config.containsValidationError(), equalTo(true));
        assertThat(config.getUniqueValidationErrorToken(), not(isEmptyString()));
        assertThat(config.getValidationErrorDescription(), containsString(config.getUniqueValidationErrorToken()));
        assertThat(config.getValidationErrorDescription(), containsString(INVALID_ROLE_1));
        assertThat(config.getValidationErrorDescription(), containsString(VALIDATION_ERROR_1.getMessage()));
        assertThat(config.getValidationErrorDescription(), containsString(INVALID_ROLE_2));
        assertThat(config.getValidationErrorDescription(), containsString(VALIDATION_ERROR_2.getMessage()));
    }

    @Test
    public void shouldReturnMultipleValidationErrorForSameRole() {
        when(rolesValidationErrors.hasErrors()).thenReturn(true);
        when(rolesValidationErrors.getErrors()).thenReturn(ImmutableMap.of(INVALID_ROLE_1, asList(VALIDATION_ERROR_1, VALIDATION_ERROR_2)));

        this.config = new DlsFlsProcessedConfig(dlsDlsConfig, documentAuthorization, fieldAuthorization, fieldMasking,null,
            rolesValidationErrors, rolesMapingdValidationErrors);

        assertThat(config.containsValidationError(), equalTo(true));
        assertThat(config.getUniqueValidationErrorToken(), not(isEmptyString()));
        assertThat(config.getValidationErrorDescription(), containsString(config.getUniqueValidationErrorToken()));
        assertThat(config.getValidationErrorDescription(), containsString(INVALID_ROLE_1));
        assertThat(config.getValidationErrorDescription(), containsString(VALIDATION_ERROR_1.getMessage()));
        assertThat(config.getValidationErrorDescription(), containsString(VALIDATION_ERROR_2.getMessage()));
    }

    @Test
    public void shouldReturnRoleValidationErrorForRolesMappings() {
        when(rolesMapingdValidationErrors.hasErrors()).thenReturn(true);
        when(rolesMapingdValidationErrors.getErrors()).thenReturn(ImmutableMap.of(INVALID_MAPPING_1, singleton(VALIDATION_ERROR_1)));

        this.config = new DlsFlsProcessedConfig(dlsDlsConfig, documentAuthorization, fieldAuthorization, fieldMasking,null,
            rolesValidationErrors, rolesMapingdValidationErrors);

        assertThat(config.containsValidationError(), equalTo(true));
        assertThat(config.getUniqueValidationErrorToken(), not(isEmptyString()));
        assertThat(config.getValidationErrorDescription(), containsString(config.getUniqueValidationErrorToken()));
        assertThat(config.getValidationErrorDescription(), containsString(INVALID_MAPPING_1));
        assertThat(config.getValidationErrorDescription(), containsString(VALIDATION_ERROR_1.getMessage()));
    }

    @Test
    public void shouldReturnMultipleRoleMappingValidationError() {
        when(rolesMapingdValidationErrors.hasErrors()).thenReturn(true);
        when(rolesMapingdValidationErrors.getErrors()).thenReturn(ImmutableMap.of(INVALID_MAPPING_1, singleton(VALIDATION_ERROR_1),//
            INVALID_MAPPING_2, singleton(VALIDATION_ERROR_2)));

        this.config = new DlsFlsProcessedConfig(dlsDlsConfig, documentAuthorization, fieldAuthorization, fieldMasking,null,
            rolesValidationErrors, rolesMapingdValidationErrors);

        assertThat(config.containsValidationError(), equalTo(true));
        assertThat(config.getUniqueValidationErrorToken(), not(isEmptyString()));
        assertThat(config.getValidationErrorDescription(), containsString(config.getUniqueValidationErrorToken()));
        assertThat(config.getValidationErrorDescription(), containsString(INVALID_MAPPING_1));
        assertThat(config.getValidationErrorDescription(), containsString(VALIDATION_ERROR_1.getMessage()));
        assertThat(config.getValidationErrorDescription(), containsString(INVALID_MAPPING_2));
        assertThat(config.getValidationErrorDescription(), containsString(VALIDATION_ERROR_2.getMessage()));
    }

    @Test
    public void shouldReturnMultipleValidationErrorForSameMappingsRole() {
        when(rolesMapingdValidationErrors.hasErrors()).thenReturn(true);
        when(rolesMapingdValidationErrors.getErrors()).thenReturn(Map.of(INVALID_MAPPING_1, asList(VALIDATION_ERROR_1, VALIDATION_ERROR_2)));

        this.config = new DlsFlsProcessedConfig(dlsDlsConfig, documentAuthorization, fieldAuthorization, fieldMasking,null,
            rolesValidationErrors, rolesMapingdValidationErrors);

        assertThat(config.containsValidationError(), equalTo(true));
        assertThat(config.getUniqueValidationErrorToken(), not(isEmptyString()));
        assertThat(config.getValidationErrorDescription(), containsString(config.getUniqueValidationErrorToken()));
        assertThat(config.getValidationErrorDescription(), containsString(INVALID_MAPPING_1));
        assertThat(config.getValidationErrorDescription(), containsString(VALIDATION_ERROR_1.getMessage()));
        assertThat(config.getValidationErrorDescription(), containsString(VALIDATION_ERROR_2.getMessage()));
    }

    @Test
    public void shouldReturnValidationErrorsForRolesAndMappings() {
        when(rolesValidationErrors.hasErrors()).thenReturn(true);
        when(rolesValidationErrors.getErrors()).thenReturn(ImmutableMap.of(INVALID_ROLE_1, singleton(VALIDATION_ERROR_1)));
        when(rolesMapingdValidationErrors.hasErrors()).thenReturn(true);
        when(rolesMapingdValidationErrors.getErrors()).thenReturn(ImmutableMap.of(INVALID_MAPPING_2, singleton(VALIDATION_ERROR_2)));

        this.config = new DlsFlsProcessedConfig(dlsDlsConfig, documentAuthorization, fieldAuthorization, fieldMasking, null,
            rolesValidationErrors, rolesMapingdValidationErrors);

        assertThat(config.containsValidationError(), equalTo(true));
        assertThat(config.getUniqueValidationErrorToken(), not(isEmptyString()));
        assertThat(config.getValidationErrorDescription(), containsString(config.getUniqueValidationErrorToken()));
        assertThat(config.getValidationErrorDescription(), containsString(INVALID_ROLE_1));
        assertThat(config.getValidationErrorDescription(), containsString(INVALID_MAPPING_2));
        assertThat(config.getValidationErrorDescription(), containsString(VALIDATION_ERROR_1.getMessage()));
        assertThat(config.getValidationErrorDescription(), containsString(VALIDATION_ERROR_2.getMessage()));
    }

}