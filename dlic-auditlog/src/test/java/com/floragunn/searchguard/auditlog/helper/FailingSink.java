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

package com.floragunn.searchguard.auditlog.helper;

import org.elasticsearch.common.settings.Settings;

import com.floragunn.searchguard.auditlog.impl.AuditMessage;
import com.floragunn.searchguard.auditlog.sink.AuditLogSink;

public class FailingSink extends AuditLogSink{

    public FailingSink(String name, Settings settings, String sinkPrefix, AuditLogSink fallbackSink) {
        super(name, settings, null, fallbackSink);
    }
    
	@Override
	protected boolean doStore(AuditMessage msg) {
		return false;
	}

    @Override
    public boolean isHandlingBackpressure() {
        return true;
    }

}
