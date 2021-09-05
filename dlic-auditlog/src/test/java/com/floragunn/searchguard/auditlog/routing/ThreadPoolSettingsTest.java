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

package com.floragunn.searchguard.auditlog.routing;

import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.auditlog.AbstractAuditlogiUnitTest;
import com.floragunn.searchguard.test.helper.file.FileHelper;

public class ThreadPoolSettingsTest extends AbstractAuditlogiUnitTest {

	@Test
	public void testNoMultipleEndpointsConfiguration() throws Exception {		
		Settings settings = Settings.builder().loadFromPath(FileHelper.getAbsoluteFilePathFromClassPath("auditlog/endpoints/sink/configuration_no_multiple_endpoints.yml")).build();
		AuditMessageRouter router = createMessageRouterComplianceEnabled(settings);		
		Assert.assertEquals(5, router.storagePool.threadPoolSize);
		Assert.assertEquals(200000, router.storagePool.threadPoolMaxQueueLen);
	}
}
