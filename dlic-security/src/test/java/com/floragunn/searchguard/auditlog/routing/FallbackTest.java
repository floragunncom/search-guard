/*
 * Copyright 2016-2018 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */

package com.floragunn.searchguard.auditlog.routing;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.auditlog.AbstractAuditlogiUnitTest;
import com.floragunn.searchguard.auditlog.helper.FailingSink;
import com.floragunn.searchguard.auditlog.helper.LoggingSink;
import com.floragunn.searchguard.auditlog.helper.MockAuditMessageFactory;
import com.floragunn.searchguard.auditlog.impl.AuditMessage;
import com.floragunn.searchguard.auditlog.impl.AuditMessage.Category;
import com.floragunn.searchguard.auditlog.sink.AuditLogSink;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.helper.file.FileHelper;

public class FallbackTest extends AbstractAuditlogiUnitTest {

	@Test
	public void testFallback() throws Exception {
		Settings.Builder settingsBuilder = Settings.builder().loadFromPath(FileHelper.getAbsoluteFilePathFromClassPath("auditlog/endpoints/routing/fallback.yml"));

		Settings settings = settingsBuilder.put("path.home", ".").put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE").put("searchguard.audit.threadpool.size", 0).build();

		AuditMessageRouter router = createMessageRouterComplianceEnabled(settings);
		
		AuditMessage msg = MockAuditMessageFactory.validAuditMessage(Category.MISSING_PRIVILEGES);
		router.route(msg);

		// endpoint 1 is failing, endoint2 and default work
		List<AuditLogSink> sinks = router.categorySinks.get(Category.MISSING_PRIVILEGES);
		Assert.assertEquals(3, sinks.size());
		// this sink has failed, message must be logged to fallback sink
		AuditLogSink sink = sinks.get(0);
		Assert.assertEquals("endpoint1", sink.getName());
		Assert.assertEquals(FailingSink.class, sink.getClass());
		sink = sink.getFallbackSink();
		Assert.assertEquals("fallback", sink.getName());
		Assert.assertEquals(LoggingSink.class, sink.getClass());
		LoggingSink loggingSkin = (LoggingSink) sink;
		Assert.assertEquals(msg, loggingSkin.messages.get(0));
		// this sink succeeds
		sink = sinks.get(1);
		Assert.assertEquals("endpoint2", sink.getName());
		Assert.assertEquals(LoggingSink.class, sink.getClass());
		loggingSkin = (LoggingSink) sink;
		Assert.assertEquals(msg, loggingSkin.messages.get(0));
		// default sink also succeeds
		sink = sinks.get(2);
		Assert.assertEquals("default", sink.getName());
		Assert.assertEquals(LoggingSink.class, sink.getClass());
		loggingSkin = (LoggingSink) sink;
		Assert.assertEquals(msg, loggingSkin.messages.get(0));

		// has only one end point which fails
		router = createMessageRouterComplianceEnabled(settings);
		msg = MockAuditMessageFactory.validAuditMessage(Category.COMPLIANCE_DOC_READ);
		router.route(msg);
		sinks = router.categorySinks.get(Category.COMPLIANCE_DOC_READ);
		sink = sinks.get(0);
		Assert.assertEquals("endpoint3", sink.getName());
		Assert.assertEquals(FailingSink.class, sink.getClass());
		sink = sink.getFallbackSink();
		Assert.assertEquals("fallback", sink.getName());
		Assert.assertEquals(LoggingSink.class, sink.getClass());
		loggingSkin = (LoggingSink) sink;
		Assert.assertEquals(msg, loggingSkin.messages.get(0));

		// has only default which succeeds
		router = createMessageRouterComplianceEnabled(settings);
		msg = MockAuditMessageFactory.validAuditMessage(Category.COMPLIANCE_DOC_WRITE);
		router.route(msg);
		sinks = router.categorySinks.get(Category.COMPLIANCE_DOC_WRITE);
		sink = sinks.get(0);
		Assert.assertEquals("default", sink.getName());
		Assert.assertEquals(LoggingSink.class, sink.getClass());
		loggingSkin = (LoggingSink) sink;
		Assert.assertEquals(1, loggingSkin.messages.size());
		Assert.assertEquals(msg, loggingSkin.messages.get(0));
		// fallback must be empty
		sink = sink.getFallbackSink();
		Assert.assertEquals("fallback", sink.getName());
		Assert.assertEquals(LoggingSink.class, sink.getClass());
		loggingSkin = (LoggingSink) sink;
		Assert.assertEquals(0, loggingSkin.messages.size());

		// test non configured categories, must be logged to default only
		router = createMessageRouterComplianceEnabled(settings);
		msg = MockAuditMessageFactory.validAuditMessage(Category.FAILED_LOGIN);
		router.route(msg);
		sinks = router.categorySinks.get(Category.FAILED_LOGIN);
		sink = sinks.get(0);
		Assert.assertEquals("default", sink.getName());
		Assert.assertEquals(LoggingSink.class, sink.getClass());
		loggingSkin = (LoggingSink) sink;
		Assert.assertEquals(1, loggingSkin.messages.size());
		Assert.assertEquals(msg, loggingSkin.messages.get(0));
		// all others must be empty
		assertLoggingSinksEmpty(router, Category.FAILED_LOGIN);

	}

	private void assertLoggingSinksEmpty(AuditMessageRouter router, Category exclude) {
		// get all sinks
		List<AuditLogSink> allSinks = router.categorySinks.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
		allSinks = allSinks.stream().filter(sink -> (sink instanceof LoggingSink)).collect(Collectors.toList());
		allSinks.removeAll(Collections.singleton(router.defaultSink));
		allSinks.removeAll(router.categorySinks.get(exclude));
		for(AuditLogSink sink : allSinks) {
			LoggingSink loggingSink = (LoggingSink)sink;
			Assert.assertEquals(0, loggingSink.messages.size());
		}
	}

}
