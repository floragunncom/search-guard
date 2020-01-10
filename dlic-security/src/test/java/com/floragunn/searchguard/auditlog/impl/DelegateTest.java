/*
 * Copyright 2016-2017 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.auditlog.impl;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.auditlog.helper.MyOwnAuditLog;
import com.floragunn.searchguard.auditlog.sink.AuditLogSink;
import com.floragunn.searchguard.auditlog.sink.DebugSink;
import com.floragunn.searchguard.auditlog.sink.InternalESSink;
import com.floragunn.searchguard.auditlog.sink.ExternalESSink;

public class DelegateTest {
	@Test
	public void auditLogTypeTest() throws Exception{
		testAuditType("DeBUg", DebugSink.class);
		testAuditType("intERnal_Elasticsearch", InternalESSink.class);
		testAuditType("EXTERnal_Elasticsearch", ExternalESSink.class);
		testAuditType("com.floragunn.searchguard.auditlog.sink.MyOwnAuditLog", MyOwnAuditLog.class);
		testAuditType("Com.Floragunn.searchguard.auditlog.sink.MyOwnAuditLog", null);
		testAuditType("idonotexist", null);
	}
		
	private void testAuditType(String type, Class<? extends AuditLogSink> expectedClass) throws Exception {
		Builder settingsBuilder  = Settings.builder();
		settingsBuilder.put("searchguard.audit.type", type);
		settingsBuilder.put("path.home", ".");
		AuditLogImpl auditLog = new AuditLogImpl(settingsBuilder.build(), null, null, null, null, null);
		auditLog.close();
//		if (expectedClass != null) {
//		    Assert.assertNotNull("delegate is null for type: "+type,auditLog.delegate);
//			Assert.assertEquals(expectedClass, auditLog.delegate.getClass());	
//		} else {
//			Assert.assertNull(auditLog.delegate);
//		}
		
	}
}
