package com.floragunn.searchguard.enterprise.dlsfls;

import com.floragunn.searchguard.enterprise.dlsfls.int_tests.DlsReadOnlyIntTests;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({DlsIntTest.class, // 11
        DlsTest.class, // 5
        DlsWriteIntTest.class, // 2
        FlsIntTest.class, // 10
        FlsKeywordTest.class, // 9
        FmIntTest.class, // 48
        InvalidRolesAndMappingConfigurationWithDisabledDlsTest.class, // 1
        DlsReadOnlyIntTests.class, // 210
        InvalidRolesAndMappingConfigurationTest.class}) // 14
public class IntegrationTestSuite {

}
