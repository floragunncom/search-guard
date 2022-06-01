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

package com.floragunn.searchguard.enterprise.auditlog.sink;

import java.io.IOException;
import java.nio.file.Path;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext.StoredContext;
import org.opensearch.threadpool.ThreadPool;

import com.floragunn.searchguard.enterprise.auditlog.impl.AuditMessage;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.HeaderHelper;

public final class InternalESSink extends AuditLogSink {

	private final Client clientProvider;
	final String index;
	final String type;
	private DateTimeFormatter indexPattern;
	private final ThreadPool threadPool;

	public InternalESSink(final String name, final Settings settings, final String settingsPrefix, final Path configPath, final Client clientProvider, ThreadPool threadPool, AuditLogSink fallbackSink) {
		super(name, settings, settingsPrefix, fallbackSink);
		this.clientProvider = clientProvider;
		Settings sinkSettings = getSinkSettings(settingsPrefix);
		
		this.index = sinkSettings.get(ConfigConstants.SEARCHGUARD_AUDIT_ES_INDEX, "'sg7-auditlog-'YYYY.MM.dd");
		this.type = sinkSettings.get(ConfigConstants.SEARCHGUARD_AUDIT_ES_TYPE, null);

		this.threadPool = threadPool;
		try {
			this.indexPattern = DateTimeFormat.forPattern(index);
		} catch (IllegalArgumentException e) {
			log.debug("Unable to parse index pattern due to {}. " + "If you have no date pattern configured you can safely ignore this message", e.getMessage());
		}
	}

	@Override
	public void close() throws IOException {

	}

	public boolean doStore(final AuditMessage msg) {

		if (Boolean.parseBoolean((String) HeaderHelper.getSafeFromHeader(threadPool.getThreadContext(), ConfigConstants.SG_CONF_REQUEST_HEADER))) {
			if (log.isTraceEnabled()) {
				log.trace("audit log of audit log will not be executed");
			}
			return true;
		}

		try (StoredContext ctx = threadPool.getThreadContext().stashContext()) {
			try {
				final IndexRequestBuilder irb = clientProvider.prepareIndex(getExpandedIndexName(indexPattern, index), type).setRefreshPolicy(RefreshPolicy.IMMEDIATE).setSource(msg.getAsMap());
				threadPool.getThreadContext().putHeader(ConfigConstants.SG_CONF_REQUEST_HEADER, "true");
				irb.setTimeout(TimeValue.timeValueMinutes(1));
				irb.execute().actionGet();
				return true;
			} catch (final Exception e) {
				log.error("Unable to index audit log {} due to {}", msg, e.toString(), e);
				return false;
			}
		}
	}
}
