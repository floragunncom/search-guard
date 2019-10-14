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

package com.floragunn.searchguard.auditlog.helper;

import java.io.IOException;
import java.nio.file.Path;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.searchguard.auditlog.impl.AuditMessage;
import com.floragunn.searchguard.auditlog.sink.AuditLogSink;

public class MyOwnAuditLog extends AuditLogSink {

	public MyOwnAuditLog(final String name, final Settings settings, final String settingsPrefix, final Path configPath, final ThreadPool threadPool,
	        final IndexNameExpressionResolver resolver, final ClusterService clusterService, AuditLogSink fallbackSink) {
        super(name, settings, settingsPrefix, fallbackSink);
    }

    @Override
	public void close() throws IOException {
		
	}

	
	public boolean doStore(AuditMessage msg) {
		return true;
	}

}
