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

package com.floragunn.searchguard.auditlog.sink;

import org.elasticsearch.common.settings.Settings;

import com.floragunn.searchguard.auditlog.sink.WebhookSink;

public class MockWebhookAuditLog extends WebhookSink {
	
	public String payload = null;
	public String url = null;
	
	public MockWebhookAuditLog(Settings settings, String settingsPrefix, AuditLogSink fallback) throws Exception {
		super("test", settings, settingsPrefix, null, fallback);
	}

	@Override
	protected boolean doPost(String url, String payload) {
		this.payload = payload;
		return true;
	}
	
	
	@Override
	protected boolean doGet(String url) {
		this.url = url;
		return true;
	}
}
