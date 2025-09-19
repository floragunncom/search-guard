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

package com.floragunn.searchguard.enterprise.auditlog.routing;

import com.floragunn.searchsupport.util.EsLogging;
import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import com.floragunn.searchguard.enterprise.auditlog.AbstractAuditlogiUnitTest;
import com.floragunn.searchguard.enterprise.auditlog.helper.LoggingSink;
import com.floragunn.searchguard.enterprise.auditlog.helper.MockAuditMessageFactory;
import com.floragunn.searchguard.enterprise.auditlog.impl.AuditMessage;
import com.floragunn.searchguard.enterprise.auditlog.impl.AuditMessage.Category;
import com.floragunn.searchguard.enterprise.auditlog.routing.AuditMessageRouter;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;



public class PerfTest extends AbstractAuditlogiUnitTest {

    @ClassRule
    public static EsLogging esLogging = new EsLogging();

	@Test
	@Ignore("jvm crash on cci")
	public void testPerf() throws Exception {
		Settings.Builder settingsBuilder = Settings.builder().loadFromPath(FileHelper.getAbsoluteFilePathFromClassPath("auditlog/endpoints/routing/perftest.yml"));

		Settings settings = settingsBuilder.put("path.home", ".")
				.put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
				.put("searchguard.audit.threadpool.size", 0)
				.build();

		AuditMessageRouter router = createMessageRouterComplianceEnabled(settings);
		int limit = 150000;
		while(limit > 0) {
			AuditMessage msg = MockAuditMessageFactory.validAuditMessage(Category.MISSING_PRIVILEGES);
			router.route(msg);
			limit--;
		}
		LoggingSink loggingSink = (LoggingSink)router.defaultSink.getFallbackSink();
		int currentSize = loggingSink.messages.size();
		Assert.assertTrue(currentSize > 0);
	}

}
