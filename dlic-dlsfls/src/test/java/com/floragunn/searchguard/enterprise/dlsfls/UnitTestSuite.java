package com.floragunn.searchguard.enterprise.dlsfls;

import com.floragunn.searchguard.enterprise.dlsfls.lucene.DocumentFilterTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({DlsFlsProcessedConfigTest.class,// 9
        FieldMaskingRuleTest.class, // 8
        RoleBasedDocumentAuthorizationTest.class,// ~2052
        RoleBasedFieldAuthorizationTest.class, // 9
        RoleBasedFieldMaskingTest.class, // 6
        DocumentFilterTest.class}) // 6
public class UnitTestSuite {
}
