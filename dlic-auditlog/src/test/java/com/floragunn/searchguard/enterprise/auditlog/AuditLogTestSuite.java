/*
 * Copyright 2018 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.auditlog;

import com.floragunn.searchguard.enterprise.auditlog.impl.AuditTestToXContentObjectImplTest;
import com.floragunn.searchguard.enterprise.auditlog.impl.AuditlogTest;
import com.floragunn.searchguard.enterprise.auditlog.impl.DelegateTest;
import com.floragunn.searchguard.enterprise.auditlog.impl.DisabledCategoriesTest;
import com.floragunn.searchguard.enterprise.auditlog.impl.IgnoreAuditUsersTest;
import com.floragunn.searchguard.enterprise.auditlog.impl.TracingTests;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.floragunn.searchguard.enterprise.auditlog.compliance.ComplianceAuditlogTest;
import com.floragunn.searchguard.enterprise.auditlog.compliance.RestApiComplianceAuditlogTest;
import com.floragunn.searchguard.enterprise.auditlog.integration.BasicAuditlogTest;
import com.floragunn.searchguard.enterprise.auditlog.integration.SSLAuditlogTest;
import com.floragunn.searchguard.enterprise.auditlog.routing.FallbackTest;
import com.floragunn.searchguard.enterprise.auditlog.routing.RouterTest;
import com.floragunn.searchguard.enterprise.auditlog.routing.RoutingConfigurationTest;
import com.floragunn.searchguard.enterprise.auditlog.routing.ThreadPoolSettingsTest;
import com.floragunn.searchguard.enterprise.auditlog.sink.SinkProviderTLSTest;
import com.floragunn.searchguard.enterprise.auditlog.sink.SinkProviderTest;
import com.floragunn.searchguard.enterprise.auditlog.sink.WebhookAuditLogTest;

@RunWith(Suite.class)

@Suite.SuiteClasses({
	ComplianceAuditlogTest.class,
	RestApiComplianceAuditlogTest.class,
	AuditlogTest.class,
	DelegateTest.class,
	DisabledCategoriesTest.class,
	IgnoreAuditUsersTest.class,
	TracingTests.class,
	BasicAuditlogTest.class,
	SSLAuditlogTest.class,
	FallbackTest.class,
	RouterTest.class,
	RoutingConfigurationTest.class,
	ThreadPoolSettingsTest.class,
	SinkProviderTest.class,
	SinkProviderTLSTest.class,
	WebhookAuditLogTest.class,
	AuditTestToXContentObjectImplTest.class
})
public class AuditLogTestSuite {

}
